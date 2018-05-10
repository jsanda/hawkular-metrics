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
package org.hawkular.metrics.api.tracing;

import java.util.EnumSet;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.annotation.WebListener;

import io.jaegertracing.Configuration;
import io.jaegertracing.Tracer;
import io.jaegertracing.samplers.ConstSampler;
import io.opentracing.contrib.jaxrs2.server.SpanFinishingFilter;
import io.opentracing.rxjava.TracingRxJavaUtils;
import io.opentracing.util.GlobalTracer;

/**
 * @author jsanda
 */
@WebListener
public class OpenTracingContextInitializer implements javax.servlet.ServletContextListener {

    private Tracer tracer;

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        tracer = initTracer("hawkular-metrics");
        GlobalTracer.register(tracer);
        TracingRxJavaUtils.enableTracing(tracer);

        ServletContext servletContext = servletContextEvent.getServletContext();
        FilterRegistration.Dynamic filterRegistration = servletContext
                .addFilter("tracingFilter", new SpanFinishingFilter(tracer));
        filterRegistration.setAsyncSupported(true);
        filterRegistration.addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), false, "*");
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        tracer.close();
    }

    private Tracer initTracer(String service) {
        Configuration.SamplerConfiguration samplerConfig = Configuration.SamplerConfiguration.fromEnv()
                .withType(ConstSampler.TYPE)
                .withParam(1);

        Configuration.ReporterConfiguration reporterConfig = Configuration.ReporterConfiguration.fromEnv()
                .withLogSpans(true)
                .withSender(Configuration.SenderConfiguration.fromEnv());

        Configuration config = new Configuration(service)
                .withSampler(samplerConfig)
                .withReporter(reporterConfig);

        return (io.jaegertracing.Tracer) config.getTracer();
    }

}