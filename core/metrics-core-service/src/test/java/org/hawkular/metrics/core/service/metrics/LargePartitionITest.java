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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import org.hawkular.metrics.model.Buckets;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.NumericBucketPoint;
import org.hawkular.metrics.model.Percentile;
import org.jboss.logging.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import rx.Completable;
import rx.Observable;

/**
 * @author jsanda
 */
public class LargePartitionITest extends BaseMetricsITest {

    private static Logger logger = Logger.getLogger(LargePartitionITest.class);

    private int ttl = Integer.parseInt(System.getProperty("ttl", "300"));

    private String tenantId = "LargePartitionTest";

    private List<MetricId<Double>> metricIds;

    private List<String> podIds;
    private List<String> hostIds;
    private List<String> groupIds;

    private int metricsToWrite = Integer.parseInt(System.getProperty("write", "2500"));

    @BeforeClass
    public void createMetrics() throws Exception {
        if (System.getProperty("skipCreate") == null) {
            int numMetrics = getProperty("metrics", 1000000);
            int numPods = getProperty("pods", 500);
            int numHosts = getProperty("hosts", 10);
            int numGroups = getProperty("numGroups", 10);

            metricIds = new ArrayList<>(numMetrics);
            podIds = new ArrayList<>(numPods);
            hostIds = new ArrayList<>(numHosts);
            groupIds = new ArrayList<>(numGroups);

            int timeout = Integer.parseInt(System.getProperty("timeout", "30"));

            String namespaceId = UUID.randomUUID().toString();
            String namespace = UUID.randomUUID().toString();
            List<String> types = asList("pod", "pod_container");

            for (int i = 0; i < numPods; ++i) {
                podIds.add(UUID.randomUUID().toString());
            }

            for (int i = 0; i < numHosts; ++i) {
                hostIds.add(UUID.randomUUID().toString());
            }

            for (int i = 0; i < numGroups; ++i) {
                groupIds.add(UUID.randomUUID().toString());
            }

            logger.infof("Creating %d metrics", numMetrics);

            int batchSize = 500;
            int numBatches = numMetrics / batchSize;
            int count = 0;

            for (int i = 0; i < numBatches; ++i) {
                List<Completable> created = new ArrayList<>(batchSize);
                for (int j = 0; j < batchSize; ++j) {
                    Map<String, String> tags = new HashMap<>();
                    tags.put("descriptor_name", groupIds.get(count % numGroups));
                    tags.put("group_id", "/" + groupIds.get(count % numGroups));
                    tags.put("host_id", hostIds.get(count % numHosts));
                    tags.put("hostname", hostIds.get(count % numHosts));
                    tags.put("namespace_id", namespaceId);
                    tags.put("namespace_name", namespace);
                    tags.put("nodename", hostIds.get(count % numHosts));
                    tags.put("pod_id", podIds.get(count % numPods));
                    tags.put("pod_name", "pod/" + podIds.get(count % numPods));
                    tags.put("pod_namespace", namespace);
                    tags.put("type", types.get(count % 2));

                    MetricId<Double> metricId = new MetricId<>(tenantId, GAUGE, UUID.randomUUID().toString());
                    Metric<Double> metric = new Metric<>(metricId, tags, null);
                    created.add(metricsService.createMetric(metric, true).toCompletable());

                    ++count;
                }
                assertTrue(Completable.merge(created).await(timeout, SECONDS));
            }

        }

        loadMetricIds(metricsToWrite);

    }

    private int getProperty(String property, int defaultValue) {
        return Integer.parseInt(System.getProperty(property, Integer.toString(defaultValue)));
    }

    private void loadMetricIds(int numIds) {
        logger.infof("Loading %d metric ids", numIds);
        Ids ids = rxSession.execute("SELECT metric, tags FROM metrics_idx WHERE tenant_id = '" + tenantId +
                "' AND type = 0 LIMIT " + numIds)
                .flatMap(Observable::from)
                .collect(Ids::new, ((theIds, row) -> {
                    MetricId<Double> metricId = new MetricId<>(tenantId, GAUGE, row.getString(0));
                    theIds.metricIds.add(metricId);
                    Map<String, String> tags = row.getMap(1, String.class, String.class);
                    theIds.groupIds.add(tags.get("group_id"));
                    theIds.hostIds.add(tags.get("host_id"));
                    theIds.podIds.add(tags.get("pod_id"));
                }))
                .toBlocking()
                .firstOrDefault(null);
        assertNotNull(ids);

        metricIds = new ArrayList<>(ids.metricIds);
        groupIds = new ArrayList<>(ids.groupIds);
        hostIds = new ArrayList<>(ids.hostIds);
        podIds = new ArrayList<>(ids.podIds);

        assertEquals(metricIds.size(), numIds);
        assertTrue(groupIds.size() > 0);
        assertTrue(hostIds.size() > 0);
        assertTrue(podIds.size() > 0);
    }

    @Test
    public void generateLoadWithLargeMetricsIdxPartition() throws Exception {
        logger.info("Starting test...");

        metricsService.setDefaultTTL(ttl);

        int numReaders = Integer.parseInt(System.getProperty("readers", "4"));
        int readInterval = Integer.parseInt(System.getProperty("readInterval", "30"));
        int writeInterval = Integer.parseInt(System.getProperty("writeInterval", "1"));

        ThreadFactory writerThreadFactory = new ThreadFactoryBuilder().setNameFormat("writer-%d").build();
        ThreadFactory readerThreadFactory = new ThreadFactoryBuilder().setNameFormat("reader-%d").build();
        ScheduledExecutorService writers = Executors.newSingleThreadScheduledExecutor(writerThreadFactory);
        ScheduledExecutorService readers = Executors.newScheduledThreadPool(numReaders, readerThreadFactory);

        writers.scheduleAtFixedRate(() -> writeData(tenantId), 0, writeInterval, SECONDS);
        for (int i = 0; i < numReaders; ++i) {
            readers.scheduleAtFixedRate(() -> readStats(tenantId), 10, readInterval, SECONDS);
        }

        while (true) {
            Thread.sleep(10000);
        }
    }

    private void readStats(String tenantId) {
        try {
            Random random = new Random();
            int bucket = random.nextInt(50);
            int shortQuery = Integer.parseInt(System.getProperty("shortQuery", "1"));
            int longQuery = Integer.parseInt(System.getProperty("longQuery", "5"));
            List<Percentile> percentiles = asList(new Percentile("0.5"), new Percentile("0.75"),
                    new Percentile("0.9"), new Percentile("0.95"), new Percentile("0.99"));
            long end = System.currentTimeMillis();
            long start = end - 5000;
            Map<String, String> tags;

            if (bucket % 2 == 0) {
//                start = end - Duration.standardMinutes(shortQuery).getMillis();
                tags = ImmutableMap.of("pod_name", "pod/" + podIds.get(random.nextInt(podIds.size())));
            } else {
                logger.debugf("Querying past %d minutes", longQuery);
//                start = end - Duration.standardMinutes(longQuery).getMillis();
                tags = ImmutableMap.of(
                        "host_id", hostIds.get(random.nextInt(hostIds.size())),
                        "group_id", groupIds.get(random.nextInt(groupIds.size()))
                );
            }

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
        } catch (Exception e) {
            logger.warn("Failed to read stats", e);
        }
    }

    private void writeData(String tenantId) {
        long timestamp = System.currentTimeMillis();
        Random random = new Random();
        List<Metric<Double>> metrics = new ArrayList<>();
        for (int i = 0; i < metricsToWrite; ++i) {
            metrics.add(new Metric<>(metricIds.get(i), singletonList(new DataPoint<>(timestamp, random.nextDouble()))));
        }
        metricsService.addDataPoints(GAUGE, Observable.from(metrics)).toCompletable().subscribe(
                () -> logger.tracef("Finished inserting %d data points for timestamp %d", metrics.size(), timestamp),
                t -> logger.warnf(t, "Failed to insert data points for timestamp %d", timestamp)
        );
    }

    private static class Ids {
        Set<MetricId<Double>> metricIds = new HashSet<>();
        Set<String> podIds = new HashSet<>();
        Set<String> hostIds = new HashSet<>();
        Set<String> groupIds = new HashSet<>();
    }

}
