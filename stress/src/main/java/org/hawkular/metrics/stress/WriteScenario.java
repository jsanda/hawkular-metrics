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
package org.hawkular.metrics.stress;

import static org.hawkular.metrics.core.api.MetricType.GAUGE;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.schedulers.Schedulers;

/**
 * @author jsanda
 */
public class WriteScenario {

    private static final Logger logger = LoggerFactory.getLogger(WriteScenario.class);

    private MetricsService metricsService;

    private Settings settings;

    private int numWriters = 8;

//    private int numMetrics = 2000;
//
//    private int dataPointsPerMetric = 2;

    private Random random = new Random();

    public WriteScenario(MetricsService metricService, Settings settings) {
        this.metricsService = metricService;
        this.settings = settings;
    }

//    public void run() {
//        logger.info("Starting scenario");
//        List<Subscription> subscriptions = new ArrayList<>(numWriters);
//        for (int i = 0; i < numWriters; ++i) {
//            Scheduler.Worker worker = Schedulers.computation().createWorker();
//            DataPump pump = new DataPump(metricsService, "WriteScenario", "M" + i, numMetrics, dataPointsPerMetric);
//            subscriptions.add(worker.schedulePeriodically(pump, 0, 10, TimeUnit.SECONDS));
//        }
//        try {
//            Thread.sleep(minutes(3).toStandardDuration().getMillis());
//        } catch (InterruptedException e) {
//            logger.info("There was an interrupt", e);
//        }
//        logger.info("Shutting down");
//        subscriptions.forEach(Subscription::unsubscribe);
//    }

    public Observable<Void> runRx() {
        logger.info("Settings = {}", settings);

        return Observable.timer(0, 3, TimeUnit.SECONDS)
                .flatMap(duration -> Observable.range(0, numWriters)
                        .flatMap(i -> metricsService.addGaugeData(createMetrics()).subscribeOn(Schedulers.io())));
    }

    private Observable<Metric<Double>> createMetrics() {
        return Observable.range(0, settings.getNumMetrics())
                .flatMap(i -> Observable.range(0, settings.getNumDataPoints())
                        .map(j -> new DataPoint<>(System.currentTimeMillis() - (j * 10), random.nextDouble()))
                        .toList()
                        .map(dataPoints -> new Metric<>("WriteScenario", GAUGE, new MetricId("M" + i), dataPoints)))
                .subscribeOn(Schedulers.computation());
    }

}
