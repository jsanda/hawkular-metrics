/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.benchmark.jmh;

import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.benchmark.jmh.util.LiveCassandraManager;
import org.hawkular.metrics.benchmark.jmh.util.MetricServiceManager;
import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import com.google.common.collect.Iterables;

/**
 * @author jsanda
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class InsertMetricTags {

    @State(Scope.Benchmark)
    public static class ServiceCreator {
        private MetricsService metricsService;
        private MetricServiceManager metricsManager;

        @Setup
        public void setup() {
            metricsManager = new MetricServiceManager(new LiveCassandraManager());
            metricsService = metricsManager.getMetricsService();
        }

        @TearDown
        public void shutdown() {
            metricsManager.shutdown();
        }

        public MetricsService getMetricsService() {
            return metricsService;
        }
    }

    @State(Scope.Benchmark)
    public static class MetricCreator {

        @Param({"false", "true"})
        private boolean batch;

        private Metric<Double> metric;

        private Map<String, String> tags;

        private List<String> descriptors = asList("uptime", "downtime", "memory/page_faults", "cpu/usage",
                "network/rx_errors", "network/tx_errors");

        private Iterator<String> descriptorIterator = Iterables.cycle(descriptors).iterator();

        @Setup(Level.Iteration)
        public void createMetric() {
            metric = new Metric<>(new MetricId<>("InsertMetricTags", MetricType.GAUGE, randomUUID().toString()));
            tags = new HashMap<>();
            String descriptor = descriptorIterator.next();
            tags.put("descriptor_name", descriptor);
            tags.put("group_id", "/" + descriptor);
            tags.put("host_id", "127.0.0.1");
            tags.put("host_name", "127.0.0.1");
            tags.put("namespace_name", "default");
            tags.put("node_name", "127.0.0.1");
            tags.put("pod_id", randomUUID().toString());
            tags.put("pod_name", randomUUID().toString());
            tags.put("type", "pod");
            tags.put("units", "jmh");
        }

        public Metric<Double> getMetric() {
            return metric;
        }

        public Map<String, String> getTags() {
            return tags;
        }

        public boolean isBatch() {
            return batch;
        }

    }

    @Benchmark
    public void addTags(ServiceCreator serviceCreator, MetricCreator metricCreator, Blackhole blackhole) {
        MetricsService metricsService = serviceCreator.getMetricsService();
        Metric<Double> metric = metricCreator.getMetric();
        Map<String, String> tags = metricCreator.getTags();
        blackhole.consume(metricsService.addTags(metric, tags, metricCreator.isBatch()).toBlocking()
                .lastOrDefault(null));
    }

}
