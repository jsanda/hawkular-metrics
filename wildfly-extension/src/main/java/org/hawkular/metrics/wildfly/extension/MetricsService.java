/*
 *
 *  * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
 *  * and other contributors as indicated by the @author tags.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.hawkular.metrics.wildfly.extension;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;

import org.jboss.logging.Logger;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;

/**
 * @author jsanda
 */
public class MetricsService implements Service<MetricsService> {

    private final Logger log = Logger.getLogger(MetricsService.class);

    private Vertx vertx;

    public void start(StartContext context) throws StartException {
        log.info("Starting hawkular metrics service");

        try {
            vertx = Vertx.vertx();
            vertx.createHttpServer(new HttpServerOptions().setHost("127.0.0.1"))
                    .requestHandler(req -> req.response().end("Hello World!"))
                    .listen(9090);
            log.info("HTTP service listening on port 9090");
        } catch (Exception e) {
            log.error("Failed to start HTTP service", e);
        }
    }

    @Override
    public void stop(StopContext context) {
        log.info("Stopping hawkular-metrics server");
    }

    @Override
    public MetricsService getValue() throws IllegalStateException, IllegalArgumentException {
        return this;
    }
}
