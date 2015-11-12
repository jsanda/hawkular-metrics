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

import static java.util.stream.Collectors.toList;

import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.enterprise.context.ApplicationScoped;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.Topic;

import org.hawkular.metrics.api.jaxrs.model.Availability;
import org.hawkular.metrics.api.jaxrs.model.AvailabilityDataPoint;
import org.hawkular.metrics.api.jaxrs.util.Eager;
import org.hawkular.metrics.core.api.AvailabilityType;
import org.hawkular.metrics.core.api.Metric;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Publishes {@link AvailDataMessage} messages on the Hawkular bus.
 *
 * @author Thomas Segismont
 */
@ApplicationScoped
@Eager
public class AvailDataPublisher {
    private static final Logger log = Logger.getLogger(AvailDataPublisher.class);

    @Resource(name = "java:/ConnectionFactory")
    private ConnectionFactory connectionFactory;

    @Resource(mappedName = "java:/jms/topic/HawkularAvailData")
    private Topic availabilityTopic;

    private ObjectMapper mapper;

    @PostConstruct
    void init() {
        // TODO use/inject a shared, configured mapper instance
        mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS, false);
        mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
    }

    public void publish(Metric<AvailabilityType> metric) {
//        BasicMessage basicMessage = createAvailMessage(metric);
//        try {
//            messageProcessor.send(producerConnectionContext, basicMessage);
//            log.tracef("Sent message: %s", basicMessage);
//        } catch (JMSException e) {
//            log.warnf(e, "Could not send metric: %s", metric);
//        }

        try (JMSContext context = connectionFactory.createContext()) {
            List<AvailabilityDataPoint> dataPoints = metric.getDataPoints().stream().map(AvailabilityDataPoint::new)
                    .collect(toList());
            Availability availability = new Availability(metric.getId().getName(), dataPoints);
            String json = mapper.writeValueAsString(availability);

            context.createProducer().send(availabilityTopic, json);
        } catch (JsonProcessingException e) {
            log.error("Failed to generate JSON", e);
        }
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
