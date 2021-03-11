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

package org.opensearch.action.admin.indices.dangling.list;

import org.opensearch.action.admin.indices.dangling.DanglingIndexInfo;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * Used when querying every node in the cluster for dangling indices, in response to a list request.
 */
public class NodeListDanglingIndicesResponse extends BaseNodeResponse {
    private final List<DanglingIndexInfo> indexMetaData;

    public List<DanglingIndexInfo> getDanglingIndices() {
        return this.indexMetaData;
    }

    public NodeListDanglingIndicesResponse(DiscoveryNode node, List<DanglingIndexInfo> indexMetaData) {
        super(node);
        this.indexMetaData = indexMetaData;
    }

    protected NodeListDanglingIndicesResponse(StreamInput in) throws IOException {
        super(in);
        this.indexMetaData = in.readList(DanglingIndexInfo::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(this.indexMetaData);
    }
}
