/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.routing.allocation.command.AllocationCommand;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Class encapsulating the explanation for a single {@link AllocationCommand}
 * taken from the Deciders
 */
public class RerouteExplanation implements ToXContentObject {

    private AllocationCommand command;
    private Decision decisions;

    public RerouteExplanation(AllocationCommand command, Decision decisions) {
        this.command = command;
        this.decisions = decisions;
    }

    public AllocationCommand command() {
        return this.command;
    }

    public Decision decisions() {
        return this.decisions;
    }

    public static RerouteExplanation readFrom(StreamInput in) throws IOException {
        AllocationCommand command = in.readNamedWriteable(AllocationCommand.class);
        Decision decisions = Decision.readFrom(in);
        return new RerouteExplanation(command, decisions);
    }

    public static void writeTo(RerouteExplanation explanation, StreamOutput out) throws IOException {
        out.writeNamedWriteable(explanation.command);
        explanation.decisions.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("command", command.name());
        builder.field("parameters", command);
        builder.startArray("decisions");
        decisions.toXContent(builder, params);
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
