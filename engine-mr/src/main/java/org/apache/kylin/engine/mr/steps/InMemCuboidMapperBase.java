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

package org.apache.kylin.engine.mr.steps;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.MemoryBudgetController;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.cuboid.DefaultCuboidScheduler;
import org.apache.kylin.cube.cuboid.TreeCuboidSchedulerManager;
import org.apache.kylin.cube.inmemcubing.ConsumeBlockingQueueController;
import org.apache.kylin.cube.inmemcubing.InputConverterUnit;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableEnrich;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CuboidStatsReaderUtil;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 */
public abstract class InMemCuboidMapperBase<KEYIN, VALUEIN, KEYOUT, VALUEOUT, T> extends KylinMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    private static final Logger logger = LoggerFactory.getLogger(InMemCuboidMapperBase.class);

    private int reserveMemoryMB;
    private int nSplit = 1;
    private int countOfLastSplit = 0;
    private int counter = 0;
    private int splitRowThreshold = Integer.MAX_VALUE;
    private int unitRows = ConsumeBlockingQueueController.DEFAULT_BATCH_SIZE;

    protected CubeInstance cube;
    protected CubeDesc cubeDesc;
    protected CubeSegment cubeSegment;
    protected Map<TblColRef, Dictionary<String>> dictionaryMap;
    protected IJoinedFlatTableDesc flatDesc;

    protected BlockingQueue<T> queue = new LinkedBlockingQueue<>(2000);
    protected InputConverterUnit<T> inputConverterUnit;
    private Future<?> future;

    protected abstract InputConverterUnit<T> getInputConverterUnit();

    protected abstract Future getCubingThreadFuture(Context context, Map<TblColRef, Dictionary<String>> dictionaryMap, int reserveMemoryMB, //
                                                    CuboidScheduler cuboidScheduler, InputConverterUnit<T> inputConverterUnit);

    protected abstract T getRecordFromKeyValue(KEYIN key, VALUEIN value);

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        Configuration conf = context.getConfiguration();

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        String cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        String segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);
        cubeSegment = cube.getSegmentById(segmentID);
        flatDesc = new CubeJoinedFlatTableEnrich(EngineFactory.getJoinedFlatTableDesc(cubeSegment), cubeDesc);

        dictionaryMap = Maps.newHashMap();

        // dictionary
        for (TblColRef col : cubeDesc.getAllColumnsHaveDictionary()) {
            Dictionary<?> dict = cubeSegment.getDictionary(col);
            if (dict == null) {
                logger.warn("Dictionary for " + col + " was not found.");
            }

            dictionaryMap.put(col, cubeSegment.getDictionary(col));
        }

        // check memory more often if a single row is big
        if (cubeDesc.hasMemoryHungryMeasures()) {
            unitRows /= 10;
        }

        String cuboidModeName = conf.get(BatchConstants.CFG_CUBOID_MODE);
        CuboidScheduler cuboidScheduler = TreeCuboidSchedulerManager.getTreeCuboidScheduler(cubeDesc, //
                CuboidStatsReaderUtil.readCuboidStatsFromSegment(cube.getCuboidsByMode(cuboidModeName), cubeSegment));
        if (cuboidScheduler == null) {
            cuboidScheduler = new DefaultCuboidScheduler(cubeDesc);
        }

        reserveMemoryMB = calculateReserveMB(conf);
        inputConverterUnit = getInputConverterUnit();
        future = getCubingThreadFuture(context, dictionaryMap, reserveMemoryMB, cuboidScheduler, inputConverterUnit);
    }

    private int calculateReserveMB(Configuration configuration) {
        int sysAvailMB = MemoryBudgetController.getSystemAvailMB();
        int mrReserve = configuration.getInt("mapreduce.task.io.sort.mb", 100);
        int sysReserve = Math.max(sysAvailMB / 10, 100);
        int reserveMB = mrReserve + sysReserve;
        logger.info("Reserve " + reserveMB + " MB = " + mrReserve + " (MR reserve) + " + sysReserve + " (SYS reserve)");
        return reserveMB;
    }

    @Override
    public void doMap(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException {
        // put each row to the queue
        T row = getRecordFromKeyValue(key, value);

        if (offer(context, row, 1, TimeUnit.MINUTES, 60)) {
            counter++;
            countOfLastSplit++;
            if (counter % BatchConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
                logger.info("Handled " + counter + " records, internal queue size = " + queue.size());
            }
        } else {
            throw new IOException("Failed to offer row to internal queue due to queue full!");
        }

        if (counter % unitRows == 0 && shouldCutSplit(nSplit, countOfLastSplit)) {
            if (offer(context, inputConverterUnit.getCutUnit(), 1, TimeUnit.MINUTES, 60)) {
                countOfLastSplit = 0;
            } else {
                throw new IOException("Failed to offer row to internal queue due to queue full!");
            }
            nSplit++;
        }
    }

    @Override
    protected void doCleanup(Context context) throws IOException, InterruptedException {
        logger.info("Totally handled " + mapCounter + " records!");

        while (!future.isDone()) {
            if (queue.offer(inputConverterUnit.getEmptyUnit(), 1, TimeUnit.SECONDS)) {
                break;
            }
        }

        futureGet(context);
        queue.clear();
    }

    private boolean shouldCutSplit(int nSplit, long splitRowCount) {
        int systemAvailMB = MemoryBudgetController.getSystemAvailMB();

        logger.info(splitRowCount + " records went into split #" + nSplit + "; " + systemAvailMB + " MB left, " + reserveMemoryMB + " MB threshold");

        if (splitRowCount >= splitRowThreshold) {
            logger.info("Split cut due to hitting splitRowThreshold " + splitRowThreshold);
            return true;
        }

        if (systemAvailMB <= reserveMemoryMB) {
            logger.info("Split cut due to hitting memory threshold, system avail " + systemAvailMB + " MB <= reserve " + reserveMemoryMB + " MB");
            return true;
        }

        return false;
    }

    private boolean offer(Context context, T row, long timeout, TimeUnit unit, int nRound) throws IOException, InterruptedException {
        while (nRound > 0) {
            if (queue.offer(row, timeout, unit)) {
                return true;
            }
            if (future.isDone()) {
                futureGet(context);
                throw new IOException("Failed to build cube in mapper due to cubing thread exit unexpectedly");
            }
            nRound--;
        }
        return false;
    }

    private void futureGet(Context context) throws IOException {
        try {
            future.get();
        } catch (Exception e) {
            throw new IOException("Failed to build cube in mapper " + context.getTaskAttemptID().getTaskID().getId(), e);
        }
    }
}
