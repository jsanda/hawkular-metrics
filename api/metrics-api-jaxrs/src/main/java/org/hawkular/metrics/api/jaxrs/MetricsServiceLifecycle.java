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
package org.hawkular.metrics.api.jaxrs;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_CQL_PORT;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_KEYSPACE;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_NODES;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.CASSANDRA_RESETDB;
import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.WAIT_FOR_SERVICE;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import org.hawkular.metrics.api.jaxrs.config.Configurable;
import org.hawkular.metrics.api.jaxrs.config.ConfigurationProperty;
import org.hawkular.metrics.api.jaxrs.util.Eager;
import org.hawkular.metrics.core.api.MetricsService;
import org.hawkular.metrics.core.impl.MetricsServiceImpl;
import org.hawkular.metrics.schema.SchemaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;

/**
 * Bean created on startup to manage the lifecycle of the {@link MetricsService} instance shared in application scope.
 *
 * @author John Sanda
 * @author Thomas Segismont
 */
@ApplicationScoped
@Eager
public class MetricsServiceLifecycle {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsServiceLifecycle.class);

    /**
     * @see #getState()
     */
    public enum State {
        STARTING, STARTED, STOPPING, STOPPED, FAILED
    }

    private final MetricsServiceImpl metricsService;
    private final ScheduledExecutorService lifecycleExecutor;

    @Inject
    @Configurable
    @ConfigurationProperty(CASSANDRA_CQL_PORT)
    private String cqlPort;

    @Inject
    @Configurable
    @ConfigurationProperty(CASSANDRA_NODES)
    private String nodes;

    @Inject
    @Configurable
    @ConfigurationProperty(CASSANDRA_KEYSPACE)
    private String keyspace;

    @Inject
    @Configurable
    @ConfigurationProperty(CASSANDRA_RESETDB)
    private String resetDb;

    @Inject
    @Configurable
    @ConfigurationProperty(WAIT_FOR_SERVICE)
    private String waitForService;

    private volatile State state;
    private int connectionAttempts;
    private Session session;

    MetricsServiceLifecycle() {
        // Create the shared instance now and initialize it with the C* session when ready
        metricsService = new MetricsServiceImpl();
        ThreadFactory threadFactory = r -> {
            Thread thread = Executors.defaultThreadFactory().newThread(r);
            thread.setName(MetricsService.class.getSimpleName().toLowerCase(Locale.ROOT) + "-lifecycle-thread");
            return thread;
        };
        // All lifecycle operations will be executed on a single thread to avoid synchronization issues
        lifecycleExecutor = Executors.newSingleThreadScheduledExecutor(threadFactory);
        state = State.STARTING;
    }

    /**
     * Returns the lifecycle state of the {@link MetricsService} shared in application scope.
     *
     * @return lifecycle state of the shared {@link MetricsService}
     */
    public State getState() {
        return state;
    }

    @PostConstruct
    void init() {
        lifecycleExecutor.submit(this::startMetricsService);
        if (Boolean.parseBoolean(waitForService)
            // "hawkular-metrics.backend" is not a real Metrics configuration parameter (there's a single
            // MetricsService implementation, which is backed by Cassandra).
            // But it's been used historically to wait for the service to be available before completing the deployment.
            // Therefore, we still use it here for backward compatibililty.
            // TODO remove when Hawkular build has been updated to use the eager startup flag
            || "embedded_cass".equals(System.getProperty("hawkular-metrics.backend"))) {
            long start = System.nanoTime();
            while (state == State.STARTING
                   // Give up after a minute. The deployment won't be failed and we'll continue to try to start the
                   // service in the background.
                   && NANOSECONDS.convert(1, MINUTES) > System.nanoTime() - start) {
                Uninterruptibles.sleepUninterruptibly(1, SECONDS);
            }
        }
    }

    private void startMetricsService() {
        if (state != State.STARTING) {
            return;
        }
        LOG.info("Initializing metrics service");
        connectionAttempts++;
        try {
            session = createSession();
        } catch (Throwable t) {
            Throwable rootCause = Throwables.getRootCause(t);
            LOG.warn("Could not connect to Cassandra cluster - assuming its not up yet", rootCause);
            // cycle between original and more wait time - avoid waiting huge amounts of time
            long delay = 1 + ((connectionAttempts - 1) % 4);
            LOG.warn("[{}] Retrying connecting to Cassandra cluster in [{}]s...", connectionAttempts, delay);
            lifecycleExecutor.schedule(this::startMetricsService, delay, SECONDS);
            return;
        }
        try {
            if (Boolean.parseBoolean(resetDb)) {
                dropKeyspace(session, keyspace);
            }
            // This creates/updates the keyspace + tables if needed
            updateSchemaIfNecessary(session, keyspace);
            session.execute("USE " + keyspace);
            LOG.info("Using a key space of '{}'", keyspace);
            metricsService.startUp(session);
            LOG.info("Metrics service started");
            state = State.STARTED;
        } catch (Throwable t) {
            LOG.error("An error occurred trying to connect to the Cassandra cluster", t);
            state = State.FAILED;
        }
    }

    private Session createSession() {
        Cluster.Builder clusterBuilder = new Cluster.Builder();
        int port;
        try {
            port = Integer.parseInt(cqlPort);
        } catch (NumberFormatException nfe) {
            String defaultPort = CASSANDRA_CQL_PORT.defaultValue();
            LOG.warn("Invalid CQL port '{}', not a number. Will use a default of {}", cqlPort, defaultPort);
            port = Integer.parseInt(defaultPort);
        }
        clusterBuilder.withPort(port);
        Arrays.stream(nodes.split(",")).forEach(clusterBuilder::addContactPoint);

        Cluster cluster = null;
        Session session = null;
        try {
            cluster = clusterBuilder.build();
            session = cluster.connect("system");
            return session;
        } finally {
            if (session == null && cluster != null) {
                cluster.close();
            }
        }
    }

    private void dropKeyspace(Session session, String keyspace) {
        session.execute("DROP KEYSPACE IF EXISTS " + keyspace);
    }

    private void updateSchemaIfNecessary(Session session, String schemaName) {
        try {
            SchemaManager schemaManager = new SchemaManager(session);
            schemaManager.createSchema(schemaName);
        } catch (IOException e) {
            throw new RuntimeException("Schema creation failed", e);
        }
    }

    /**
     * @return a {@link MetricsService} instance to share in application scope
     */
    @Produces
    @ApplicationScoped
    public MetricsService getMetricsService() {
        return metricsService;
    }

    @PreDestroy
    void destroy() {
        Future stopFuture = lifecycleExecutor.submit(this::stopMetricsService);
        try {
            Futures.get(stopFuture, 1, MINUTES, Exception.class);
        } catch (Exception ignore) {
        }
        lifecycleExecutor.shutdown();
    }

    private void stopMetricsService() {
        state = State.STOPPING;
        if (session != null) {
            try {
                session.close();
                session.getCluster().close();
            } finally {
                state = State.STOPPED;
            }
        }
    }
}
