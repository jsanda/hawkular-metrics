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
package org.hawkular.metrics.core.impl;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import static org.hawkular.metrics.model.MetricType.GAUGE;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.hawkular.metrics.core.service.metrics.BaseMetricsITest;
import org.hawkular.metrics.datetime.DateTimeService;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.Percentile;
import org.hawkular.metrics.model.param.BucketConfig;
import org.hawkular.metrics.model.param.TimeRange;
import org.jboss.logging.Logger;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.testng.annotations.Test;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import rx.Observable;
import rx.subjects.PublishSubject;

/**
 * @author jsanda
 */
public class LongTermStorageITest extends BaseMetricsITest {

    private static Logger logger = Logger.getLogger(LongTermStorageITest.class);

    final String tenantId = "LTS";

    Random random = new Random();

    ExecutorService compressionJobs;

    PublishSubject<Metric<?>> subject = PublishSubject.create();

    @Test
    public void queryLongTermData() throws Exception {
        int numMetrics = 25;
        DateTime end = DateTimeService.current24HourTimeSlice();
        DateTime start = end.minusYears(1).minusMonths(1);

        List<MetricId<Double>> ids = new ArrayList<>(numMetrics);
        for (int i = 0; i < numMetrics; ++i) {
            ids.add(new MetricId<>(tenantId, GAUGE, "M" + i));
        }

        if (Boolean.getBoolean("generate-data")) {
            generateData(ids, end, start);
        }

        BucketConfig bucketConfig = new BucketConfig(null, new org.hawkular.metrics.model.param.Duration(1,
                TimeUnit.DAYS), new TimeRange(start.getMillis(), end.getMillis()));
        List<Percentile> percentiles = asList(new Percentile("50.0"), new Percentile("90.0"), new Percentile("99.0"),
                new Percentile("99.9"));

        for (int i = 0; i < 10; ++i) {
            ids.forEach(id -> {
                Stopwatch stopwatch = Stopwatch.createStarted();
                metricsService.findGaugeStats(id, bucketConfig, percentiles)
                        .doOnNext(bucketPoints -> logger.infof("Retrieved %s", bucketPoints))
                        .toCompletable()
                        .await();
                stopwatch.stop();
                logger.infof("Query completed in %d ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
            });
        }
    }

    private void generateData(List<MetricId<Double>> ids, DateTime end, DateTime start) throws Exception {
        DateTime time = start;
        DateTime nextCompressionTime = time.plusHours(2);
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("compression-thread-pool-%d").build();
        compressionJobs = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
                threadFactory, new ThreadPoolExecutor.DiscardPolicy());

        while (time.isBefore(end)) {
            Observable<Metric<Double>> dataPoints = createDataPoints(ids, time);
            metricsService.addDataPoints(GAUGE, dataPoints)
                    .toCompletable()
                    .await();
            time = time.plusMinutes(1);
            if (time.equals(DateTimeService.getTimeSlice(time, Duration.standardDays(1)))) {
                logger.infof("Current day is %s", time.toDate());
            }
            if (time.equals(nextCompressionTime)) {
                nextCompressionTime = nextCompressionTime.plusHours(2);
                submitCompressionJob(ids, time);
            }
        }

        compressionJobs.shutdown();
        compressionJobs.awaitTermination(1, TimeUnit.MINUTES);
    }

    private Observable<Metric<Double>> createDataPoints(List<MetricId<Double>> ids, DateTime time) {
        return Observable.from(ids.stream()
                .map(id -> new Metric<>(id, singletonList(new DataPoint<>(time.getMillis(),
                Math.abs(random.nextDouble()) % 100.0d))))
                .collect(Collectors.toList()));
    }

    private void submitCompressionJob(List<MetricId<Double>> ids, DateTime end) {
        compressionJobs.submit(() -> {
            try {
                metricsService.compressBlock(Observable.from(ids), end.minusHours(2).getMillis(),
                        end.getMillis(), 2000, subject).await();
            } catch (Exception e) {
                logger.warnf(e, "Compression for %s failed", end.minusHours(2).toDate());
            }
        });
    }


}
