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

package org.apache.kylin.tool.metrics.systemcube;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Map;
import java.util.UUID;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.tool.metrics.systemcube.util.KafkaSinkTool;
import org.apache.kylin.tool.metrics.systemcube.util.StreamSinkTool;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

public class StreamingCreator {

    public static void main(String[] args) throws Exception {
        //        KylinConfig.setSandboxEnvIfPossible();
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        StreamingConfig streamingConfig = generateKylinStreamingForMetricsQuery(config, new KafkaSinkTool());
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(buf);
        StreamingConfig.SERIALIZER.serialize(streamingConfig, dout);
        dout.close();
        buf.close();
        System.out.println(buf.toString());
    }

    public static StreamingConfig generateKylinStreamingForMetricsQuery(KylinConfig config, StreamSinkTool sinkTool) {
        String tableName = sinkTool.getTableNameForMetrics(config.getKylinMetricsSubjectQuery());
        return generateKylinStreaming(tableName, sinkTool.getTopicName(tableName), sinkTool.getStreamingType(),
                sinkTool.getStreamingProperties());
    }

    public static StreamingConfig generateKylinStreamingForMetricsQueryCube(KylinConfig config,
            StreamSinkTool sinkTool) {
        String tableName = sinkTool.getTableNameForMetrics(config.getKylinMetricsSubjectQueryCube());
        return generateKylinStreaming(tableName, sinkTool.getTopicName(tableName), sinkTool.getStreamingType(),
                sinkTool.getStreamingProperties());
    }

    public static StreamingConfig generateKylinStreamingForMetricsQueryRPC(KylinConfig config,
            StreamSinkTool sinkTool) {
        String tableName = sinkTool.getTableNameForMetrics(config.getKylinMetricsSubjectQueryRpcCall());
        return generateKylinStreaming(tableName, sinkTool.getTopicName(tableName), sinkTool.getStreamingType(),
                sinkTool.getStreamingProperties());
    }

    public static StreamingConfig generateKylinStreamingForMetricsJob(KylinConfig config, StreamSinkTool sinkTool) {
        String tableName = sinkTool.getTableNameForMetrics(config.getKylinMetricsSubjectJob());
        return generateKylinStreaming(tableName, sinkTool.getTopicName(tableName), sinkTool.getStreamingType(),
                sinkTool.getStreamingProperties());
    }

    public static StreamingConfig generateKylinStreamingForMetricsJobException(KylinConfig config,
            StreamSinkTool sinkTool) {
        String tableName = sinkTool.getTableNameForMetrics(config.getKylinMetricsSubjectJobException());
        return generateKylinStreaming(tableName, sinkTool.getTopicName(tableName), sinkTool.getStreamingType(),
                sinkTool.getStreamingProperties());
    }

    public static StreamingConfig generateKylinStreaming(String tableName, String topicName, String type,
            Map<String, String> properties) {
        StreamingConfig streamingConfig = new StreamingConfig();
        streamingConfig.setUuid(UUID.randomUUID().toString());
        streamingConfig.setName(tableName);
        streamingConfig.setLastModified(0L);
        streamingConfig.setType(type);

        Map<String, String> streamingProperties = Maps.newHashMap(properties);
        streamingProperties.put("topic", topicName);
        streamingConfig.setProperties(streamingProperties);

        return streamingConfig;
    }

    public static class StreamingConfig extends RootPersistentEntity {

        public static final String STREAMING_TYPE_KAFKA = "kafka";
        public static Serializer<StreamingConfig> SERIALIZER = new JsonSerializer<StreamingConfig>(
                StreamingConfig.class);
        @JsonProperty("name")
        private String name;

        @JsonProperty("type")
        private String type = STREAMING_TYPE_KAFKA;

        @JsonProperty("properties")
        private Map<String, String> properties = Maps.newLinkedHashMap();

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Map<String, String> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, String> properties) {
            this.properties = properties;
        }

    }
}
