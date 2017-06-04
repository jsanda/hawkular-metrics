/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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

package org.hawkular.metrics.core.service.transformers;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.hawkular.metrics.model.Buckets;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.NumericBucketPoint;
import org.hawkular.metrics.model.Percentile;
import org.jboss.logging.Logger;

import com.google.common.base.Stopwatch;

import rx.Observable;
import rx.Observable.Transformer;

/**
 * @author Thomas Segismont
 */
public class NumericBucketPointTransformer
        implements Transformer<DataPoint<? extends Number>, List<NumericBucketPoint>> {

    private static Logger logger = Logger.getLogger(NumericBucketPointTransformer.class);

    private final Buckets buckets;
    private final List<Percentile> percentiles;

    public NumericBucketPointTransformer(Buckets buckets, List<Percentile> percentiles) {
        this.buckets = buckets;
        this.percentiles = percentiles;
    }

    @Override
    public Observable<List<NumericBucketPoint>> call(Observable<DataPoint<? extends Number>> dataPoints) {
        AtomicReference<Stopwatch> stopwach = new AtomicReference<>();
        return dataPoints
                .doOnSubscribe(() -> stopwach.set(Stopwatch.createStarted()))
                .groupBy(dataPoint -> buckets.getIndex(dataPoint.getTimestamp()))
                .flatMap(group -> group.collect(()
                                -> new NumericDataPointCollector(buckets, group.getKey(), percentiles),
                        NumericDataPointCollector::increment))
                .map(NumericDataPointCollector::toBucketPoint)
                .toMap(NumericBucketPoint::getStart)
//                .map(pointMap -> NumericBucketPoint.toList(pointMap, buckets))
                .map(pointMap -> {
                    stopwach.get().stop();
                    logger.infof("Finished transforming data points in %d ms",
                            stopwach.get().elapsed(MILLISECONDS));
                    return NumericBucketPoint.toList(pointMap, buckets);
                });
//                .doOnCompleted(() -> {
//                        stopwach.get().stop();
//                        logger.infof("Finished transforming data points in %d ms",
//                                stopwach.get().elapsed(MILLISECONDS));
//                });
    }
}
