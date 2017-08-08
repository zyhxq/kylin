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

package org.apache.kylin.metrics.query;

import org.apache.kylin.metrics.lib.impl.RecordEvent;
import org.apache.kylin.metrics.lib.impl.RecordEventWrapper;

import com.google.common.base.Strings;

public class RPCRecordEventWrapper extends RecordEventWrapper {

    public RPCRecordEventWrapper(RecordEvent metricsEvent) {
        super(metricsEvent);
    }

    public void setRPCCallWrapper(String projectName, String realizationName, String rpcServer) {
        this.metricsEvent.put(PropertyEnum.EXCEPTION.toString(), "NULL");
        this.metricsEvent.put(PropertyEnum.PROJECT.toString(), projectName);
        this.metricsEvent.put(PropertyEnum.REALIZATION.toString(), realizationName);
        this.metricsEvent.put(PropertyEnum.RPC_SERVER.toString(), rpcServer);
    }

    public void setRPCCallStats(long callTimeMs, long skipCount, long scanSize, long returnSize, long aggrSize) {
        this.metricsEvent.put(PropertyEnum.CALL_TIME.toString(), callTimeMs);
        this.metricsEvent.put(PropertyEnum.SKIP_COUNT.toString(), skipCount); //Number of skips on region servers based on region meta or fuzzy filter
        this.metricsEvent.put(PropertyEnum.SCAN_SIZE.toString(), scanSize); //Size scanned by region server
        this.metricsEvent.put(PropertyEnum.RETURN_SIZE.toString(), returnSize);//Size returned by region server
        this.metricsEvent.put(PropertyEnum.AGGR_FILTER_SIZE.toString(), scanSize - returnSize); //Size filtered and aggregated by coprocessor
        this.metricsEvent.put(PropertyEnum.AGGR_SIZE.toString(), aggrSize); //Size aggregated by coprocessor
    }

    public <T extends Throwable> void setStats(Class<T> exceptionClassName) {
        this.metricsEvent.put(PropertyEnum.EXCEPTION.toString(), exceptionClassName.getName());
    }

    public enum PropertyEnum {
        PROJECT("PROJECT"), REALIZATION("REALIZATION"), RPC_SERVER("RPC_SERVER"), EXCEPTION("EXCEPTION"), //
        CALL_TIME("CALL_TIME"), SKIP_COUNT("COUNT_SKIP"), SCAN_SIZE("SIZE_SCAN"), RETURN_SIZE(
                "SIZE_RETURN"), AGGR_FILTER_SIZE("SIZE_AGGREGATE_FILTER"), AGGR_SIZE("SIZE_AGGREGATE");

        private final String propertyName;

        PropertyEnum(String name) {
            this.propertyName = name;
        }

        public static PropertyEnum getByName(String name) {
            if (Strings.isNullOrEmpty(name)) {
                return null;
            }
            for (PropertyEnum property : PropertyEnum.values()) {
                if (property.propertyName.equals(name.toUpperCase())) {
                    return property;
                }
            }

            return null;
        }

        @Override
        public String toString() {
            return propertyName;
        }
    }

}