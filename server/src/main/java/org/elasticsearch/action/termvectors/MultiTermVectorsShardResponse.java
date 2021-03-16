/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.termvectors;

import com.carrotsearch.hppc.IntArrayList;
import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MultiTermVectorsShardResponse extends ActionResponse {

    final IntArrayList locations;
    final List<TermVectorsResponse> responses;
    final List<MultiTermVectorsResponse.Failure> failures;

    MultiTermVectorsShardResponse() {
        locations = new IntArrayList();
        responses = new ArrayList<>();
        failures = new ArrayList<>();
    }

    MultiTermVectorsShardResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        locations = new IntArrayList(size);
        responses = new ArrayList<>(size);
        failures = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            locations.add(in.readVInt());
            if (in.readBoolean()) {
                responses.add(new TermVectorsResponse(in));
            } else {
                responses.add(null);
            }
            if (in.readBoolean()) {
                failures.add(new MultiTermVectorsResponse.Failure(in));
            } else {
                failures.add(null);
            }
        }
    }

    public void add(int location, TermVectorsResponse response) {
        locations.add(location);
        responses.add(response);
        failures.add(null);
    }

    public void add(int location, MultiTermVectorsResponse.Failure failure) {
        locations.add(location);
        responses.add(null);
        failures.add(failure);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(locations.size());
        for (int i = 0; i < locations.size(); i++) {
            out.writeVInt(locations.get(i));
            if (responses.get(i) == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                responses.get(i).writeTo(out);
            }
            if (failures.get(i) == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                failures.get(i).writeTo(out);
            }
        }
    }
}
