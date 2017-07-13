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
package org.hawkular.metrics.server.vertx;

import static java.util.Arrays.asList;

import static org.hawkular.metrics.model.MetricType.GAUGE;

import static io.vertx.core.http.HttpMethod.POST;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.hawkular.metrics.core.dropwizard.HawkularMetricRegistry;
import org.hawkular.metrics.core.dropwizard.MetricNameService;
import org.hawkular.metrics.core.service.DataAccess;
import org.hawkular.metrics.core.service.DataAccessImpl;
import org.hawkular.metrics.core.service.MetricsServiceImpl;
import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.fasterxml.jackson.AvailabilityTypeDeserializer;
import org.hawkular.metrics.model.fasterxml.jackson.AvailabilityTypeKeySerializer;
import org.hawkular.metrics.model.fasterxml.jackson.AvailabilityTypeSerializer;
import org.hawkular.metrics.model.fasterxml.jackson.MetricTypeDeserializer;
import org.hawkular.metrics.schema.SchemaService;
import org.hawkular.metrics.sysconfig.ConfigurationService;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.hawkular.rx.cassandra.driver.RxSessionImpl;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpMethod;
import io.vertx.rx.java.RxHelper;


/**
 * @author jsanda
 */
public class HttpServerTest {

    protected static final int DEFAULT_TTL = 7; //days

    Vertx vertx;

    Session session;

    HawkularMetricRegistry metricRegistry;

    MetricsServiceImpl metricsService;

    DataAccess dataAccess;

    ObjectMapper mapper;

    @BeforeClass
    public void init() {
        String nodeAddresses = System.getProperty("nodes", "127.0.0.1");
        Cluster cluster = new Cluster.Builder()
                .addContactPoints(nodeAddresses.split(","))
                .withQueryOptions(new QueryOptions().setRefreshSchemaIntervalMillis(0))
                .build();
        session = cluster.connect();

        RxSession rxSession = new RxSessionImpl(session);

        SchemaService schemaService = new SchemaService();
        schemaService.run(session, getKeyspace(), Boolean.valueOf(System.getProperty("resetdb", "true")));

        session.execute("USE " + getKeyspace());

        metricRegistry = new HawkularMetricRegistry();
        metricRegistry.setMetricNameService(new MetricNameService());

        dataAccess = new DataAccessImpl(session);

        ConfigurationService configurationService = new ConfigurationService() ;
        configurationService.init(rxSession);

        metricsService = new MetricsServiceImpl();
        metricsService.setDataAccess(dataAccess);
        metricsService.setConfigurationService(configurationService);
        metricsService.setDefaultTTL(DEFAULT_TTL);
        metricsService.startUp(session, getKeyspace(), true, metricRegistry);

        mapper = createMapper();

        vertx = Vertx.vertx();
        vertx.deployVerticle(new HttpServer(metricsService));
    }

    private ObjectMapper createMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS, false);
        mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);

        SimpleModule module = new SimpleModule();
        module.addDeserializer(AvailabilityType.class, new AvailabilityTypeDeserializer());
        module.addDeserializer(MetricType.class, new MetricTypeDeserializer());
        module.addSerializer(AvailabilityType.class, new AvailabilityTypeSerializer());
        module.addKeySerializer(AvailabilityType.class, new AvailabilityTypeKeySerializer());
        mapper.registerModule(module);

        return mapper;
    }

    protected static String getKeyspace() {
        return System.getProperty("keyspace", "hawkulartest");
    }

    @Test
    public void insertGaugeDataPoints() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        String tenantId = "InsertGaugeDataPoints";
        DateTime start = DateTime.now().minusMinutes(30);
        Metric<Double> m1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m1"), asList(
                new DataPoint<>(start.getMillis(), 1.1),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 3.3),
                new DataPoint<>(start.plusMinutes(6).getMillis(), 4.4)));
        TypeReference<List<Metric<Double>>> typeRef = new TypeReference<List<Metric<Double>>>() {};
        String json = mapper.writerFor(typeRef).writeValueAsString(Collections.singletonList(m1));

        HttpClient httpClient = vertx.createHttpClient();
        HttpClientRequest request = httpClient.request(POST, 8282, "localhost", "/hawkular/metrics/gauges/raw");
        RxHelper.toObservable(request)
                .subscribe(
                        response -> System.out.println("Status: " + response.statusCode()),
                        t -> {
                            t.printStackTrace();
                            latch.countDown();
                        },
                        latch::countDown
                );
        request.setChunked(true);
        request.write(json);
        request.end();
        latch.await();
    }

}
