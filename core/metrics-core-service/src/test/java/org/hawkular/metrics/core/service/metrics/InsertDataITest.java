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

import static org.hawkular.metrics.core.service.metrics.BaseMetricsITest.DEFAULT_TTL;
import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.joda.time.DateTime.now;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.core.service.DataAccess;
import org.hawkular.metrics.core.service.DataAccessImpl;
import org.hawkular.metrics.core.service.MetricsServiceImpl;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.sysconfig.ConfigurationService;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.hawkular.rx.cassandra.driver.RxSessionImpl;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import rx.Observable;

/**
 * @author jsanda
 */
public class InsertDataITest {

//    @Test
    public void insertDataPoints() throws Exception {
        String nodeAddresses = System.getProperty("nodes", "127.0.0.1");
        Cluster cluster = new Cluster.Builder()
                .addContactPoints(nodeAddresses.split(","))
//                .withProtocolVersion(ProtocolVersion.V4)
                .build();
        Session session = cluster.connect();
        RxSession rxSession = new RxSessionImpl(session);

        session.execute("USE hawkular_metrics");

        DataAccess dataAccess = new DataAccessImpl(session);

        ConfigurationService configurationService = new ConfigurationService() ;
        configurationService.init(rxSession);

        MetricsServiceImpl metricsService = new MetricsServiceImpl();
        metricsService.setDataAccess(dataAccess);
        metricsService.setConfigurationService(configurationService);
        metricsService.setDefaultTTL(DEFAULT_TTL);
        metricsService.startUp(session, "hawkular_metrics", true, new MetricRegistry());

        Random random = new Random();

        String tenantId = "com.acme";
        MetricId<Double> metricId = new MetricId<>(tenantId, GAUGE, "request_size");
        DateTime time  = now().minusDays(40);
        DateTime end = time.plusDays(30);
        List<DataPoint<Double>> dataPoints = new ArrayList<>();

        while (time.isBefore(end)) {
            dataPoints.add(new DataPoint<>(time.getMillis(), random.nextDouble()));
            time = time.plusMinutes(1);
        }

        metricsService.addDataPoints(GAUGE, Observable.just(new Metric<>(metricId, dataPoints))).toCompletable()
                .await(30, TimeUnit.SECONDS);
    }

    @Test
    public void query() throws Exception {
        URL url = new URL("http", "localhost", 8080,
                "/hawkular/metrics/gauges/request_size/raw?start1428867145579");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Hawkular-Tenant", "com.acme");
        BufferedReader rd = new BufferedReader(new InputStreamReader(connection.getInputStream()));
//        String line;
//        while ((line = rd.readLine()) != null) {
//            System.out.println("LINE: " + line);
//        }
        rd.close();
    }

}
