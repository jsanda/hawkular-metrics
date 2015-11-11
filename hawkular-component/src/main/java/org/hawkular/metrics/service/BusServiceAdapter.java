/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkular.metrics.service;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Resource;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Alternative;
import javax.inject.Inject;
import javax.jms.CompletionListener;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;

import org.hawkular.alerts.api.json.JsonUtil;
import org.hawkular.alerts.api.model.event.Alert;
import org.hawkular.alerts.api.services.AlertsCriteria;
import org.hawkular.alerts.api.services.AlertsQuery;
import org.hawkular.metrics.api.jaxrs.model.DetailedMetricDefinition;
import org.hawkular.metrics.api.jaxrs.model.MetricDefinition;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricsService;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;

import rx.Observable;

/**
 * @author jsanda
 */
@ApplicationScoped
@Alternative
public class BusServiceAdapter implements MetricsServiceAdapter {

    private static final Logger log = Logger.getLogger(BusServiceAdapter.class);

    @Inject
    private MetricsService metricsService;

    @Resource(name = "java:/ConnectionFactory")
    private ConnectionFactory connectionFactory;

    @Resource(mappedName = "java:/jms/queue/AlertsQueries")
    private Queue alertsQueriesQueue;

    @Resource
    private ManagedExecutorService executorService;

    @Override public <T> Observable<? extends MetricDefinition> findMetric(MetricId<T> metricId, boolean detailed) {
        Observable<Metric<T>> metricObservable = metricsService.findMetric(metricId);
        if (detailed) {
            Observable<List<Alert>> alertsObservable = findAlertsAsync(metricId);
            return Observable.zip(metricObservable, alertsObservable, DetailedMetricDefinition::new);
        } else {
            return metricObservable.map(MetricDefinition::new);
        }
    }

    private <T> Observable<List<Alert>> findAlertsAsync(MetricId<T> metricId) {
        logMessage("Creating alerts observable");

        AlertsCriteria criteria = new AlertsCriteria();
        criteria.setTags(ImmutableMap.of("metric", metricId.getName()));
        AlertsQuery alertsQuery = new AlertsQuery(metricId.getTenantId(), criteria);

        JMSContext context = connectionFactory.createContext();
        TemporaryQueue responseQueue = context.createTemporaryQueue();
        JMSProducer producer = context.createProducer().setJMSReplyTo(responseQueue);
        JMSConsumer consumer = context.createConsumer(responseQueue);
        Observable<Message> request = send(producer, alertsQueriesQueue, JsonUtil.toJson(alertsQuery));
        Observable<String> response = receive(request, consumer);

        return response.map(json -> JsonUtil.fromJson(json, new TypeReference<List<Alert>>() {}, true))
                .finallyDo(() -> close(context));
    }

    private <T> Observable<List<Alert>> findAlerts(MetricId<T> metricId) {
        logMessage("Creating alerts observable");
        return Observable.create(subscriber -> {
            AtomicReference<JMSContext> contextRef = new AtomicReference<JMSContext>();
            try {
                contextRef.set(connectionFactory.createContext());
//                context = connectionFactory.createContext();
                AlertsCriteria criteria = new AlertsCriteria();
                criteria.setTags(ImmutableMap.of("metric", metricId.getName()));
                AlertsQuery alertsQuery = new AlertsQuery(metricId.getTenantId(), criteria);
                TemporaryQueue responseQueue = contextRef.get().createTemporaryQueue();
                logMessage("Submitting alerts query message");
                contextRef.get().createProducer().setJMSReplyTo(responseQueue).send(alertsQueriesQueue,
                        JsonUtil.toJson(alertsQuery));

                JMSConsumer consumer = contextRef.get().createConsumer(responseQueue);
                consumer.setMessageListener(message -> {
                    logMessage("Alerts query response received");
                    try {
                        String response = ((TextMessage) message).getText();
//                    String response = consumer.receiveBody(String.class);
                        List<Alert> alerts = JsonUtil.fromJson(response, new TypeReference<List<Alert>>() {}, true);
                        subscriber.onNext(alerts);
                        close(contextRef.get());
                        subscriber.onCompleted();
                    } catch (JMSException e) {
                        subscriber.onError(e);
                        close(contextRef.get());
                    }
                });
            } catch (Exception e) {
                subscriber.onError(e);
                close(contextRef.get());
            }
        });
    }

    private Observable<Message> send(JMSProducer producer, Destination destination, String json) {
        return Observable.create(subscriber -> {
            producer.setAsync(new CompletionListener() {
                @Override public void onCompletion(Message message) {
                    subscriber.onNext(message);
                    subscriber.onCompleted();
                }

                @Override public void onException(Message message, Exception exception) {
                    subscriber.onError(exception);
                }
            });
            producer.send(destination, json);
        });
    }

    private Observable<String> receive(Observable<Message> request, JMSConsumer consumer) {
        return Observable.create(subscriber ->
            request.subscribe(
                    requestMsg ->
                            consumer.setMessageListener(responseMsg -> {
                                try {
                                    String json = ((TextMessage) responseMsg).getText();
                                    subscriber.onNext(json);
                                    subscriber.onCompleted();
                                } catch (JMSException e) {
                                    subscriber.onError(e);
                                }
                        }),
                    subscriber::onError
            )
        );
    }

    private void logMessage(String msg) {
        log.debug("[" + Thread.currentThread().getName() + "] " + msg);
    }

    private void close(AutoCloseable closeable) {
        executorService.submit(() -> {
            try {
                logMessage("Closing " + closeable);
                if (closeable != null) {
                    closeable.close();
                }
            } catch (Exception e) {
                log.warn("Failed to close " + closeable, e);
            }
        });
    }

}
