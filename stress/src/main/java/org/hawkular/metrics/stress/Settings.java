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

import java.util.List;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import org.apache.commons.cli.CommandLine;

/**
 * @author jsanda
 */
public class Settings {

    private int port;

    private List<String> nodes;

    private String keyspace;

    private int numWriters;

    private int numMetrics;

    private int numDataPoints;

    private int numMinutes;

    public Settings(CommandLine cmdLine) {
        port = Integer.parseInt(cmdLine.getOptionValue("c", "9042"));
        nodes = ImmutableList.copyOf(cmdLine.getOptionValue("n", "127.0.0.1").split(","));
        keyspace = cmdLine.getOptionValue("k", "hawkular_metrics_stress");
        numMetrics = Integer.parseInt(cmdLine.getOptionValue("m"));
        numDataPoints = Integer.parseInt(cmdLine.getOptionValue("p"));
        numMinutes = Integer.parseInt(cmdLine.getOptionValue("t"));

    }

    public int getPort() {
        return port;
    }

    public List<String> getNodes() {
        return nodes;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public int getNumMetrics() {
        return numMetrics;
    }

    public int getNumDataPoints() {
        return numDataPoints;
    }

    public int getNumMinutes() {
        return numMinutes;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("port", port)
                .add("keyspace", keyspace)
                .add("numMetris", numMetrics)
                .add("numDataPoints", numDataPoints)
                .add("numMinutes", numMinutes)
                .toString();
    }
}
