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

import javax.annotation.Resource;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.Topic;

import org.hawkular.metrics.api.jaxrs.model.Availability;
import org.hawkular.metrics.api.jaxrs.model.AvailabilityDataPoint;
import org.hawkular.metrics.api.jaxrs.util.Eager;
import org.hawkular.metrics.core.api.AvailabilityType;
import org.hawkular.metrics.core.api.Metric;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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

    @Inject
    private ObjectMapper mapper;

    public void publish(Metric<AvailabilityType> metric) {
        try (JMSContext context = connectionFactory.createContext()) {
            List<AvailabilityDataPoint> dataPoints = metric.getDataPoints().stream().map(AvailabilityDataPoint::new)
                    .collect(toList());
            Availability availability = new Availability(metric.getId().getName(), dataPoints);
            String json = mapper.writeValueAsString(availability);

            context.createProducer().send(availabilityTopic, json);
        } catch (JsonProcessingException e) {
            log.warnf(e, "Could not send metric: %s", metric);
        }
    }

}
