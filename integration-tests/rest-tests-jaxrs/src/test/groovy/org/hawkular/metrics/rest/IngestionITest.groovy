/*
 * Copyright 2014-2018 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.rest

import groovy.json.JsonOutput
import io.jaegertracing.Configuration
import io.jaegertracing.Tracer
import io.jaegertracing.samplers.ConstSampler
import io.opentracing.Scope
import io.opentracing.propagation.Format
import io.opentracing.propagation.TextMap
import io.opentracing.tag.Tags
import io.opentracing.util.GlobalTracer
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test

import static org.junit.Assert.assertEquals

/**
 * @author jsanda
 */
class IngestionITest extends RESTTest {

    class Metric {
        String id
        List data
    }

    class DataPoint {
        long timestamp
        double value
    }

    class TextMapCarrier implements TextMap {

        Map headers = [:]

        @Override
        Iterator<Map.Entry<String, String>> iterator() {
            throw new UnsupportedOperationException("carrier is write-only")
        }

        @Override
        void put(String key, String value) {
            headers[key] = value
        }
    }

    static Tracer tracer

    @BeforeClass
    static void setup() {
        tracer = initTracer()
        GlobalTracer.register(tracer)
    }

    @AfterClass
    static void tearDown() {
        tracer.close()
    }

    @Test
    void largeDataSet() {
        String tenantId = nextTenantId()
        long timestamp = System.currentTimeMillis()
        Random random = new Random()

        def metrics = []
        100000.times { i -> metrics << new Metric(
                id: "M$i",
                data: [new DataPoint(timestamp: timestamp, value: Math.abs(random.nextDouble()))])
        }

        doWithSpan("insert-raw-data", {
            Tags.SPAN_KIND.set(tracer.activeSpan(), Tags.SPAN_KIND_CLIENT)
            Tags.HTTP_METHOD.set(tracer.activeSpan(), "POST")
            Tags.HTTP_URL.set(tracer.activeSpan(), "gauges/raw")
            def carrier = new TextMapCarrier()
            tracer.inject(tracer.activeSpan().context(), Format.Builtin.HTTP_HEADERS, carrier)
            carrier.headers[tenantHeaderName] = tenantId

            def response = hawkularMetrics.post(
                    path: "gauges/raw",
                    headers: carrier.headers,
                    body: metrics
            )
            assertEquals(200, response.status)
        })
    }

    static def doWithSpan(String operation, Closure work) {
        def scope = tracer.buildSpan(operation).startActive(true)
        try {
            work()
        } finally {
            scope.span().finish()
        }
    }

    static Tracer initTracer() {
        def samplerConfig = Configuration.SamplerConfiguration.fromEnv()
                .withType(ConstSampler.TYPE)
                .withParam(1)

        def reporterConfig = Configuration.ReporterConfiguration.fromEnv()
                .withLogSpans(true)

        def config = new Configuration("hawkular-metrics")
                .withSampler(samplerConfig)
                .withReporter(reporterConfig);

        return config.tracer
    }

}
