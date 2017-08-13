/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.tool.metrics.systemcube.util;

import java.util.Map;

import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metrics.lib.impl.RecordEvent;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

public abstract class StreamSinkTool {

    @JsonProperty("cubeDescOverrideProperties")
    protected Map<String, String> cubeDescOverrideProperties = Maps.newHashMap();

    @JsonProperty("streamingProperties")
    protected Map<String, String> streamingProperties = Maps.newHashMap();

    public abstract String getTableNameForMetrics(String subject);

    public int getStorageType() {
        return IStorageAware.ID_STREAM;
    }

    public abstract String getStreamingType();

    public String getTopicName(String tableName) {
        return tableName.replace('.', '_');
    }

    public int getSourceType() {
        return ISourceAware.ID_STREAMING;
    }

    public Map<String, String> getCubeDescOverrideProperties() {
        return cubeDescOverrideProperties;
    }

    public void setCubeDescOverrideProperties(Map<String, String> cubeDescOverrideProperties) {
        this.cubeDescOverrideProperties = cubeDescOverrideProperties;
    }

    public void setStreamingProperties(Map<String, String> streamingProperties) {
        this.streamingProperties = streamingProperties;
    }

    public Map<String, String> getStreamingProperties() {
        return streamingProperties;
    }

    public void init() {
        streamingProperties.put("parserProperties",
                "tsColName=" + RecordEvent.RecordReserveKeyEnum.TIME.toString() + ";");
    }
}
