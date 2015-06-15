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
package org.hawkular.metrics.stress;

import static org.joda.time.DateTime.now;
import static org.joda.time.Minutes.minutes;
import static org.joda.time.Minutes.minutesBetween;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Slf4jReporter;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.hawkular.metrics.core.impl.MetricsServiceImpl;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jsanda
 */
public class Main {

    private static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        try {
            initLogging();

            CommandLineParser parser = new DefaultParser();
            CommandLine cmdLine = parser.parse(getOptions(), args);

            Settings settings = new Settings(cmdLine);
            MetricRegistry metricRegistry = new MetricRegistry();

            Slf4jReporter reporter = Slf4jReporter
                    .forRegistry(metricRegistry)
                    .outputTo(LoggerFactory.getLogger("org.hawkular.metrics.stress"))
                    .convertRatesTo(TimeUnit.SECONDS)
                    .build();
            reporter.start(1, TimeUnit.MINUTES);

            Cluster cluster = Cluster.builder()
                    .addContactPoints(settings.getNodes().toArray(new String[settings.getNodes().size()]))
                    .withPort(settings.getPort())
                    .build();
            Session session = cluster.connect();

            MetricsServiceImpl metricsService = new MetricsServiceImpl();
            metricsService.startUp(session, settings.getKeyspace(), true, metricRegistry);

            DateTime startTime = now();
            WriteScenario scenario = new WriteScenario(metricsService, settings);
            scenario.runRx()
                    .takeUntil(aVoid ->
                            minutesBetween(startTime, now()).isGreaterThan(minutes(settings.getNumMinutes())))
                    .subscribe(
                            noArg -> System.out.println("tick..."),
                            t -> {
                                logger.error("There was an error executing the scenario", t);
                                shutdown(reporter, session, metricsService, 1);
                            },
                            () -> shutdown(reporter, session, metricsService, 0)
                    );
        } catch (ParseException parseException) {
            printUsage();
            System.exit(1);
        } catch (Exception e) {
            logger.error("There was an unexpected error", e);
            System.exit(1);
        }
    }

    private static void shutdown(ScheduledReporter reporter, Session session, MetricsServiceImpl metricsService,
            int status) {
        metricsService.shutdown();
        session.close();
        reporter.stop();
        System.exit(status);
    }

    private static Options getOptions() {
        Option cqlPort = new Option("c", "cql-port", true, "The CQL port on which Cassandra listens for client " +
                "requests.");
        Option nodes = new Option("n", "nodes", true, "A comma-delimited list of Cassandra node endpoint addresses.");
        Option keyspace = new Option("k", "keyspace", true, "The keyspace to use. It will be created if it does not " +
                "exist.");
        Option metrics = new Option("m", "metrics", true, "The number of metrics to include in each call to insert " +
                "data.");
        Option dataPoints = new Option("p", "data-points", true, "The number of data points per metric in each call " +
                "to insert data.");
        Option time = new Option("t","time", true, "The length of time specified in minutes that the stress tool " +
                "should run.");

        return new Options()
                .addOption(new Option("h", "help", false, "Show this message."))
                .addOption(cqlPort)
                .addOption(nodes)
                .addOption(keyspace)
                .addOption(metrics)
                .addOption(dataPoints)
                .addOption(time);
    }

    private static void printUsage() {
        HelpFormatter helpFormatter = new HelpFormatter();
        String syntax = "java -jar metrics-stress.jar [options]";
        String header = "";

        helpFormatter.printHelp(syntax, header, getHelpOptions(), null);
    }

    private static Options getHelpOptions() {
        Options helpOptions = new Options();
        getOptions().getOptions().forEach(helpOptions::addOption);
        return helpOptions;
    }

    private static void initLogging() {
        ConsoleAppender console = new ConsoleAppender();
        String pattern = "%-5p %d{dd-MM HH:mm:ss,SSS} (%F:%M:%L) - %m%n";
        console.setLayout(new PatternLayout(pattern));
        console.setThreshold(Level.INFO);
        console.activateOptions();

        org.apache.log4j.Logger.getRootLogger().addAppender(console);
    }

}
