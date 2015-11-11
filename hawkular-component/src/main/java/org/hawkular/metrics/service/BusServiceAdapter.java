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

import javax.annotation.Resource;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Alternative;
import javax.inject.Inject;
import javax.jms.Queue;

import org.hawkular.alerts.api.json.JsonUtil;
import org.hawkular.alerts.api.model.event.Alert;
import org.hawkular.alerts.api.services.AlertsCriteria;
import org.hawkular.alerts.api.services.AlertsQuery;
import org.hawkular.metrics.api.jaxrs.model.DetailedMetricDefinition;
import org.hawkular.metrics.api.jaxrs.model.MetricDefinition;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.MetricsService;
import org.hawkular.metrics.service.messaging.RxBusUtil;
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

    @Resource(mappedName = "java:/jms/queue/AlertsQueries")
    private Queue alertsQueriesQueue;

    @Inject
    private RxBusUtil rxBusUtil;

    @Resource
    private ManagedExecutorService executorService;

    @Override public <T> Observable<Void> addDataPoints(MetricType<T> metricType, Observable<Metric<T>> metrics) {
        return null;
    }

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

        return rxBusUtil.sendAndReceive(alertsQueriesQueue, JsonUtil.toJson(alertsQuery))
                .map(json -> JsonUtil.fromJson(json, new TypeReference<List<Alert>>() {}, true));
    }

    private void logMessage(String msg) {
        log.debug("[" + Thread.currentThread().getName() + "] " + msg);
    }

}
