/*
 * Copyright 2016 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics;

import static java.util.Collections.singletonList;

import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.LocalBean;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;
import javax.jms.Queue;

import org.hawkular.bus.Bus;
import org.hawkular.metrics.component.publish.MetricDataMessage;
import org.jboss.logging.Logger;

/**
 * @author jsanda
 */
@Singleton
@Startup
@LocalBean
public class DataPublisher {

    private static final Logger log = Logger.getLogger(DataPublisher.class);

    @Inject
    private Bus bus;

    @Resource(mappedName = "java:/queue/hawkular/metrics/gauges/new")
    Queue gaugesQueue;

    @PostConstruct
    public void init() {
        log.info("PUBLISHER RUNNING");
//        MetricDataMessage.SingleMetric metric = new MetricDataMessage.SingleMetric("TEST_GAUGE",
//                System.currentTimeMillis(), 3.14);
//        MetricDataMessage msg = new MetricDataMessage(metric);
        MetricDataMessage.MetricData metricData = new MetricDataMessage.MetricData();
        metricData.setTenantId("PUBLISH_TEST");
        List<MetricDataMessage.SingleMetric> numericList = singletonList(new MetricDataMessage.SingleMetric(
                "TEST_GAUGE", System.currentTimeMillis(), 3.14));
        metricData.setData(numericList);

        bus.send(gaugesQueue, new MetricDataMessage(metricData)).subscribe(
                m -> log.info("Sent message " + m),
                t -> log.warn("Failed to publish message", t),
                () -> log.info("Finished publishing metric")
        );
    }

}
