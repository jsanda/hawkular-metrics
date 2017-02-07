/*
 * Copyright 2017 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.service.metrics;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

import org.hawkular.metrics.model.Buckets;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.NumericBucketPoint;
import org.hawkular.metrics.model.Percentile;
import org.jboss.logging.Logger;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import rx.Completable;
import rx.Observable;

/**
 * @author jsanda
 */
public class StatsITest extends BaseMetricsITest {

    private static Logger logger = Logger.getLogger(StatsITest.class);

    @Test
    public void getStats() throws Exception {
        metricsService.setDefaultTTL(Integer.parseInt(System.getProperty("ttl", "300")));

        String tenantId = "STATS";
        int numMetrics = Integer.parseInt(System.getProperty("metrics", "2000"));
        int numMetricsPerQuery = 50;
        int numReaders = 4;
        List<Completable> created = new ArrayList<>(numMetrics);
//        Map<String, String> tags = ImmutableMap.of("x", "1", "y", "2");
        List<MetricId<Double>> metricIds = new ArrayList<>(numMetrics);

        if (Boolean.parseBoolean(System.getProperty("createMetrics", "true"))) {
            for (int i = 0; i < numMetrics; ++i) {
                MetricId<Double> metricId = new MetricId<>(tenantId, GAUGE, "M" + i);
                metricIds.add(metricId);
                int bucket = i % numMetricsPerQuery;
                Metric<Double> metric = new Metric<>(metricId, ImmutableMap.of("bucket", Integer.toString(bucket)), null);
                created.add(metricsService.createMetric(metric, true).toCompletable());
            }

            assertTrue(Completable.merge(created).await(30, SECONDS));
        }

//        session.execute("alter table data WITH default_time_to_live = 300");
//        session.execute("ALTER TABLE data with gc_grace_seconds = 10800");

//        DateTime end = DateTime.now();
//        DateTime start = end.minusDays(2);

//        generateData(tenantId, numMetrics, start, end);

        logger.info("Finished generating data");

        int numThreads = 4;

        ThreadFactory writerThreadFactory = new ThreadFactoryBuilder().setNameFormat("writer-%d").build();
        ThreadFactory readerThreadFactory = new ThreadFactoryBuilder().setNameFormat("reader-%d").build();
        ScheduledExecutorService writers = Executors.newSingleThreadScheduledExecutor(writerThreadFactory);
        ScheduledExecutorService readers = Executors.newScheduledThreadPool(numReaders, readerThreadFactory);

//        ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(numThreads, readerThreadFactory);

        writers.scheduleAtFixedRate(() -> writeData(tenantId, numMetrics), 0, 1, SECONDS);
        for (int i = 0; i < numReaders; ++i) {
            readers.scheduleAtFixedRate(() -> readStats(tenantId), 10, 30, SECONDS);
        }

//        for (int i = 0; i < numThreads; ++i) {
//            Runnable job = () -> fetchStats(tenantId, tags, start, end);
//            threadPool.scheduleAtFixedRate(job, 1, 10, SECONDS);
//        }

        while (true) {
            Thread.sleep(10000);
        }
    }

//    private void fetchStats(String tenantId, Map<String, String> tags, DateTime start, DateTime end) {
////        try {
//            int count = 0;
//
////            while (count == 0) {
//                List<Percentile> percentiles = asList(new Percentile("0.5"), new Percentile("0.75"),
//                        new Percentile("0.9"), new Percentile("0.95"), new Percentile("0.99"));
//
//                List<NumericBucketPoint> stats = metricsService.findMetricsWithFilters(tenantId, GAUGE, tags)
//                        .map(Metric::getMetricId)
//                        .toList()
//                        .flatMap(ids -> metricsService.findNumericStats(tenantId, GAUGE, tags,
//                                start.plusDays(1).getMillis(), end.getMillis(), Buckets.fromCount(start.plusDays(1).getMillis(), end.getMillis(), 60),
//                                percentiles, false))
//                        .toBlocking()
//                        .firstOrDefault(Collections.emptyList());
//
//                assertTrue(!stats.isEmpty());
////                Thread.sleep(10000);
////            }
////        } catch (InterruptedException e) {
////            fail();
////        }
//    }

    private void readStats(String tenantId) {
        Random random = new Random();
        int bucket = random.nextInt(50);
        List<Percentile> percentiles = asList(new Percentile("0.5"), new Percentile("0.75"),
                new Percentile("0.9"), new Percentile("0.95"), new Percentile("0.99"));
        long end = System.currentTimeMillis();
        long start;
        if (bucket % 2 == 0) {
            start = end - (60000 * 4);
        } else {
            start = end - (60000 * 11);
            logger.debug("Querying past 5 minutes");
        }
        Map<String, String> tags = ImmutableMap.of("bucket", Integer.toString(bucket));

        List<NumericBucketPoint> stats = metricsService.findMetricsWithFilters(tenantId, GAUGE, tags)
                .map(Metric::getMetricId)
                .toList()
                .flatMap(ids -> metricsService.findNumericStats(tenantId, GAUGE, tags, start, end,
                        Buckets.fromCount(start, end, 60), percentiles, false))
                .toBlocking()
                .firstOrDefault(Collections.emptyList());

        if (stats.isEmpty()) {
            logger.warnf("Did not find any stats for bucket %d", bucket);
        }
    }

    private void writeData(String tenantId, int numMetrics) {
        long timestamp = System.currentTimeMillis();
        Random random = new Random();
        List<Metric<Double>> metrics = new ArrayList<>();
        for (int i = 0; i < numMetrics; ++i) {
            metrics.add(new Metric<>(new MetricId<>(tenantId, GAUGE, "M" + i),
                    singletonList(new DataPoint<>(timestamp, random.nextDouble()))));
        }
        metricsService.addDataPoints(GAUGE, Observable.from(metrics)).toCompletable().subscribe(
                () -> logger.tracef("Finished inserting %d data points for timestamp %d", metrics.size(), timestamp),
                t -> logger.warnf(t, "Failed to insert data points for timestamp %d", timestamp)
        );
    }

    public void generateData(String tenantId, int numMetrics, DateTime start, DateTime end) throws Exception {
        AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
        DateTime time = start;

        while (time.isBefore(end)) {
            CountDownLatch latch = new CountDownLatch(1);
            List<Observable<Void>> inserted = new ArrayList<>();
            List<Metric<Double>> metrics = new ArrayList<>();
            for (int i = 0; i < numMetrics; ++i) {
                List<DataPoint<Double>> dataPoints = asList(
                        new DataPoint<>(time.getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(10).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(20).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(30).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(40).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(50).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(60).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(70).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(80).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(90).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(100).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(110).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(120).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(130).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(140).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(150).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(160).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(170).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(180).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(190).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(200).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(210).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(220).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(230).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(240).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(250).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(260).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(270).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(280).getMillis(), 3.14),
                        new DataPoint<>(time.plusSeconds(290).getMillis(), 3.14)
                );
                metrics.add(new Metric<>(new MetricId<>(tenantId, GAUGE, "M" + i), dataPoints));
            }
            inserted.add(metricsService.addDataPoints(GAUGE, Observable.from(metrics)));
            Observable.merge(inserted).subscribe(
                    aVoid -> {
                    },
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
//            logger.info("Current time is [" + time.toDate() + "]");
        }
    }

}
