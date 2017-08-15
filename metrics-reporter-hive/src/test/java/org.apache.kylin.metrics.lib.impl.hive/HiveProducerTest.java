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

package org.apache.kylin.metrics.lib.impl.hive;

import java.io.File;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.metrics.lib.Record;
import org.apache.kylin.metrics.lib.impl.RecordEvent;
import org.apache.kylin.metrics.lib.impl.TimedRecordEvent;
import org.apache.kylin.metrics.property.QueryRPCPropertyEnum;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HiveProducerTest {

    public static String SANDBOX_TEST_DATA = "../examples/test_case_data/sandbox";

    @Before
    public void setUp() throws Exception {
        ClassUtil.addClasspath(new File(SANDBOX_TEST_DATA).getAbsolutePath());
    }

    @After
    public void after() throws Exception {
    }

    @Test
    public void testProduce() throws Exception {
        Configuration config = HadoopUtil.getCurrentConfiguration();
        Properties props = new Properties();
        props.put("dfs.replication", 1);
        HiveConf hiveConf = new HiveConf(config, HiveProducerTest.class);
        HiveProducer hiveProducer = new HiveProducer(props, hiveConf);
        hiveProducer.send(generateTestRecord());
    }

    private Record generateTestRecord() {
        RecordEvent rpcMetricsEvent = new TimedRecordEvent("metrics_query_rpc_test");
        setRPCWrapper(rpcMetricsEvent, "default", "test_cube", "sandbox", null);
        setRPCStats(rpcMetricsEvent, 80L, 0L, 3L, 3L, 0L);
        return rpcMetricsEvent;
    }

    private static void setRPCWrapper(RecordEvent metricsEvent, String projectName, String realizationName,
            String rpcServer, Throwable throwable) {
        metricsEvent.put(QueryRPCPropertyEnum.PROJECT.toString(), projectName);
        metricsEvent.put(QueryRPCPropertyEnum.REALIZATION.toString(), realizationName);
        metricsEvent.put(QueryRPCPropertyEnum.RPC_SERVER.toString(), rpcServer);
        metricsEvent.put(QueryRPCPropertyEnum.EXCEPTION.toString(),
                throwable == null ? "NULL" : throwable.getClass().getName());
    }

    private static void setRPCStats(RecordEvent metricsEvent, long callTimeMs, long skipCount, long scanCount,
            long returnCount, long aggrCount) {
        metricsEvent.put(QueryRPCPropertyEnum.CALL_TIME.toString(), callTimeMs);
        metricsEvent.put(QueryRPCPropertyEnum.SKIP_COUNT.toString(), skipCount); //Number of skips on region servers based on region meta or fuzzy filter
        metricsEvent.put(QueryRPCPropertyEnum.SCAN_COUNT.toString(), scanCount); //Count scanned by region server
        metricsEvent.put(QueryRPCPropertyEnum.RETURN_COUNT.toString(), returnCount);//Count returned by region server
        metricsEvent.put(QueryRPCPropertyEnum.AGGR_FILTER_COUNT.toString(), scanCount - returnCount); //Count filtered & aggregated by coprocessor
        metricsEvent.put(QueryRPCPropertyEnum.AGGR_COUNT.toString(), aggrCount); //Count aggregated by coprocessor
    }
}
