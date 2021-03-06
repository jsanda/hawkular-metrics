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
package org.hawkular.metrics.api.jaxrs.callback;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.hawkular.metrics.api.jaxrs.ApiError;
import org.hawkular.metrics.api.jaxrs.NoResultsException;
import org.hawkular.metrics.core.api.MetricAlreadyExistsException;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;

/**
 * @author John Sanda
 */
public class NoDataCallback<T> implements FutureCallback<T> {

    protected AsyncResponse response;

    public NoDataCallback(AsyncResponse response) {
        this.response = response;
    }

    @Override
    public void onSuccess(Object result) {
        response.resume(Response.ok().type(MediaType.APPLICATION_JSON_TYPE).build());
    }

    @Override
    public void onFailure(Throwable t) {
        if (t instanceof MetricAlreadyExistsException) {
            ApiError errors = new ApiError(
                    "A metric input id already exists " + ":"
                    + Throwables.getRootCause(t).getMessage());
            response.resume(Response.status(Status.BAD_REQUEST).entity(errors).type(APPLICATION_JSON_TYPE).build());
        } else if (t instanceof NoResultsException) {
            response.resume(Response.ok().status(Status.NO_CONTENT).build());
        } else {
            ApiError errors = new ApiError(
                    "Failed to perform operation due to an error: "
                    + Throwables.getRootCause(t).getMessage());
            response.resume(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(errors)
                    .type(MediaType.APPLICATION_JSON_TYPE).build());
        }
    }
}
