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

import static org.hawkular.metrics.core.api.MetricType.AVAILABILITY;
import static org.hawkular.metrics.core.api.MetricType.GAUGE;

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.hawkular.metrics.api.jaxrs.ServiceReady;
import org.hawkular.metrics.api.jaxrs.util.Eager;
import org.hawkular.metrics.core.api.AvailabilityType;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricsService;
import org.jboss.logging.Logger;

import rx.Observable;
import rx.Subscription;
import rx.schedulers.Schedulers;

/**
 * Subscribes to {@link MetricsService#insertedDataEvents()} and relays metrics to a bus publisher.
 *
 * @author Thomas Segismont
 */
@ApplicationScoped
@Eager
public class InsertedDataSubscriber {
    private static final Logger log = Logger.getLogger(InsertedDataSubscriber.class);

    @Inject
    MetricDataPublisher metricDataPublisher;
    @Inject
    AvailDataPublisher availDataPublisher;

    private Subscription subscription;

    @SuppressWarnings("unused")
    public void onMetricsServiceReady(@Observes @ServiceReady MetricsService metricsService) {
        Observable<List<Metric<?>>> events = metricsService.insertedDataEvents()
                .buffer(50, TimeUnit.MILLISECONDS, 100)
                .filter(list -> !list.isEmpty())
                .onBackpressureBuffer()
                .observeOn(Schedulers.io());
        subscription = events.subscribe(list -> list.forEach(this::onInsertedData));
    }

    private void onInsertedData(Metric<?> metric) {
        log.tracef("Inserted metric: %s", metric);
        if (metric.getId().getType() == AVAILABILITY) {
            @SuppressWarnings("unchecked")
            Metric<AvailabilityType> avail = (Metric<AvailabilityType>) metric;
            availDataPublisher.publish(avail);
        } else if (metric.getId().getType() == GAUGE) {
            @SuppressWarnings("unchecked")
            Metric<Double> numeric = (Metric<Double>) metric;
            metricDataPublisher.publish(numeric);
        }
    }

    @PreDestroy
    void shutdown() {
        if (subscription != null) {
            subscription.unsubscribe();
        }
    }
}
