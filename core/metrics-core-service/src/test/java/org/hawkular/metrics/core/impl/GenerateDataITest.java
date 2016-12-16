/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.impl;

import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.testng.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.hawkular.metrics.core.jobs.CompressData;
import org.hawkular.metrics.core.service.metrics.BaseMetricsITest;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.jboss.logging.Logger;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import rx.Completable;
import rx.Observable;

/**
 * This test is disabled because it is really just a utility for generating a bunch of data.
 *
 * @author jsanda
 */
public class GenerateDataITest extends BaseMetricsITest {

    private static Logger logger = Logger.getLogger(GenerateDataITest.class);

    private final String tenantId = "T0";

    private int numMetrics = Integer.parseInt(System.getProperty("metrics", "5000"));;

    private AtomicReference<Throwable> exceptionRef = new AtomicReference<>();

    private RandomDataGenerator dataGenerator = new RandomDataGenerator();

    @Test//(enabled = false)
    public void generateData() throws Exception {
        DateTime end = new DateTime(2016, 12, 13, 9, 0);
        int numDays = Integer.parseInt(System.getProperty("days", "7"));
        DateTime time = end.minusDays(numDays);

        if (System.getProperty("compress") == null) {
            logger.info("Generating " + numDays + " days of raw data");
            generateRawData(time, end);
        } else {
            logger.info("Compressing " + numDays + " days of raw data");
            compressData(time, end);
        }

    }

    private void generateRawData(DateTime time, DateTime end) throws Exception {
        while (time.isBefore(end)) {
            CountDownLatch latch = new CountDownLatch(1);
            List<Observable<Void>> inserted = new ArrayList<>();
            List<Metric<Double>> metrics = new ArrayList<>();
            for (int j = 0; j < numMetrics; ++j) {
                List<DataPoint<Double>> dataPoints = new ArrayList<>();
                for (int k = 0; k < 300; k += 10) {
                    dataPoints.add(new DataPoint<>(time.getMillis(), dataGenerator.nextUniform(0.01, 250.0)));
                }
                metrics.add(new Metric<>(new MetricId<>(tenantId, GAUGE, "M" + j), dataPoints));
            }
            inserted.add(metricsService.addDataPoints(GAUGE, Observable.from(metrics)));
            Observable.merge(inserted).subscribe(
                    aVoid -> {},
                    t -> {
                        logger.error("Inserting data failed", t);
                        exceptionRef.set(t);
                        latch.countDown();
                    },
                    latch::countDown
            );
            latch.await();
            assertNull(exceptionRef.get());
            time = time.plusMinutes(5);
            logger.info("Current time is [" + time.toDate() + "]");
        }
    }

    private void compressData(DateTime time, DateTime end) {
        List<MetricId<Double>> metricIdsList = new ArrayList<>();
        for (int i = 0; i < numMetrics; ++i) {
            metricIdsList.add(new MetricId<>(tenantId, GAUGE, "M" + i));
        }
        Observable<MetricId<Double>> metricIds = Observable.from(metricIdsList);

        while (time.isBefore(end)) {
            DateTime next = time.plus(CompressData.DEFAULT_BLOCK_SIZE);
            Completable completable = metricsService.compressBlock(metricIds, time.getMillis(), next.getMillis(),
                    COMPRESSION_PAGE_SIZE);
            time = next;
            completable.await();
            logger.info("Current time is [" + time.toDate() + "]");
        }
    }

}
