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
package org.hawkular.metrics.service.messaging;

import javax.annotation.Resource;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.jms.CompletionListener;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;

import org.jboss.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import rx.Observable;

/**
 * @author jsanda
 */
@ApplicationScoped
public class RxBusUtil {

    private static final Logger log = Logger.getLogger(RxBusUtil.class);

    @Resource(name = "java:/ConnectionFactory")
    private ConnectionFactory connectionFactory;

    @Resource
    private ManagedExecutorService executorService;

    @Inject
    private ObjectMapper mapper;

    public RxBusUtil() {
    }

    /**
     * Asynchronously sends a value to the specified {@link Destination destination}. The value is first serialized
     * into JSON as a String object.
     *
     * @return An {@link Observable observable} that emit the message that was sent
     */
    public <T> Observable<Message> sendJson(Destination destination, T value) {
        JMSContext context = connectionFactory.createContext();
        JMSProducer producer = context.createProducer();
        return Observable.create(subscriber -> {
            try {
                String json = mapper.writeValueAsString(value);
                producer.setAsync(new CompletionListener() {
                    @Override public void onCompletion(Message message) {
                        subscriber.onNext(message);
                        close(context);
                        subscriber.onCompleted();
                    }

                    @Override public void onException(Message message, Exception exception) {
                        subscriber.onError(exception);
                        close(context);
                    }
                });
                producer.send(destination, json);
            } catch (JsonProcessingException e) {
                subscriber.onError(e);
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

    public Observable<String> sendAndReceive(Destination destination, String json) {
        JMSContext context = connectionFactory.createContext();
        TemporaryQueue responseQueue = context.createTemporaryQueue();
        JMSProducer producer = context.createProducer().setJMSReplyTo(responseQueue);
        JMSConsumer consumer = context.createConsumer(responseQueue);

        Observable<Message> request = send(producer, destination, json);

        return receive(request, consumer).finallyDo(() -> close(context));
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
