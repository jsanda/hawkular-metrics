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

import static org.hawkular.metrics.model.MetricType.GAUGE;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.fasterxml.jackson.AvailabilityTypeDeserializer;
import org.hawkular.metrics.model.fasterxml.jackson.AvailabilityTypeKeySerializer;
import org.hawkular.metrics.model.fasterxml.jackson.AvailabilityTypeSerializer;
import org.hawkular.metrics.model.fasterxml.jackson.MetricTypeDeserializer;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.Router;
import io.vertx.rx.java.RxHelper;
import rx.Completable;
import rx.Observable;
import rx.exceptions.Exceptions;
import rx.observables.SyncOnSubscribe;

/**
 * @author jsanda
 */
public class HttpServer extends AbstractVerticle {

    private static final String BASE_URL = "/hawkular/metrics/";

    private MetricsService metricsService;

    private ObjectMapper mapper;

    public HttpServer(MetricsService metricsService) {
        this.metricsService = metricsService;
        mapper = createMapper();
    }

    @Override
    public void start() throws Exception {
        Router router = Router.router(vertx);

        router.route(HttpMethod.POST, BASE_URL + "gauges/raw").handler(context ->
                  RxHelper.toObservable(context.request())
                    .map(this::getMetrics)
                    .flatMap(gaugesObservable -> metricsService.addDataPoints(GAUGE, gaugesObservable))
                    .toCompletable()
                    .subscribe(
                            () -> context.response()
                                    .setStatusCode(HttpResponseStatus.OK.code())
                                    .end(),
                            context::fail
                    )
        );

        vertx.createHttpServer(new HttpServerOptions())
                .requestHandler(router::accept)
                .listen(8282);
    }

    private Observable<Metric<Double>> getMetrics(Buffer buffer) {
        return Observable.fromCallable(() -> {
            TypeReference<List<Metric<Double>>> typeRef = new TypeReference<List<Metric<Double>>>() {};
            List<Metric<Double>> gauges =
                    mapper.reader(typeRef).readValue(buffer.getBytes());
            return gauges;
        }).flatMap(Observable::from);
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

}
