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

package org.hawkular.metrics.component.publish;

import javax.enterprise.context.ApplicationScoped;

import org.hawkular.metrics.api.jaxrs.util.Eager;
import org.hawkular.metrics.core.api.AvailabilityType;
import org.hawkular.metrics.core.api.Metric;

/**
 * Publishes {@link AvailDataMessage} messages on the Hawkular bus.
 *
 * @author Thomas Segismont
 */
@ApplicationScoped
@Eager
public class AvailDataPublisher {
//    private static final Logger log = Logger.getLogger(AvailDataPublisher.class);
//
//    static final String HAWULAR_AVAIL_DATA_TOPIC = "HawkularAvailData";
//
//    @Resource(mappedName = "java:/HawkularBusConnectionFactory")
//    TopicConnectionFactory topicConnectionFactory;
//
//    private MessageProcessor messageProcessor;
//    private ConnectionContextFactory connectionContextFactory;
//    private ProducerConnectionContext producerConnectionContext;
//
//    @PostConstruct
//    void init() {
//        messageProcessor = new MessageProcessor();
//        try {
//            connectionContextFactory = new ConnectionContextFactory(topicConnectionFactory);
//            Endpoint endpoint = new Endpoint(TOPIC, HAWULAR_AVAIL_DATA_TOPIC);
//            producerConnectionContext = connectionContextFactory.createProducerConnectionContext(endpoint);
//        } catch (JMSException e) {
//            throw new RuntimeException(e);
//        }
//    }

    public void publish(Metric<AvailabilityType> metric) {
//        BasicMessage basicMessage = createAvailMessage(metric);
//        try {
//            messageProcessor.send(producerConnectionContext, basicMessage);
//            log.tracef("Sent message: %s", basicMessage);
//        } catch (JMSException e) {
//            log.warnf(e, "Could not send metric: %s", metric);
//        }
    }

//    private BasicMessage createAvailMessage(Metric<AvailabilityType> avail) {
//        MetricId<AvailabilityType> availId = avail.getId();
//        List<AvailDataMessage.SingleAvail> availList = avail.getDataPoints().stream()
//                .map(dataPoint -> new AvailDataMessage.SingleAvail(availId.getTenantId(), availId.getName(),
//                        dataPoint.getTimestamp(),
//                        dataPoint.getValue().getText().toUpperCase()))
//                .collect(toList());
//        AvailDataMessage.AvailData metricData = new AvailDataMessage.AvailData();
//        metricData.setData(availList);
//        return new AvailDataMessage(metricData);
//    }
//
//    @PreDestroy
//    void shutdown() {
//        if (producerConnectionContext != null) {
//            try {
//                producerConnectionContext.close();
//            } catch (IOException ignored) {
//            }
//        }
//        if (connectionContextFactory != null) {
//            try {
//                connectionContextFactory.close();
//            } catch (JMSException ignored) {
//            }
//        }
//    }
}
