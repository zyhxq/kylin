package org.apache.kylin.storage.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * <p/>
 * This tool serves for the purpose of
 * checking the "KYLIN_HOST" property to be consistent with the dst's MetadataUrlPrefix
 * for all of cube segments' corresponding HTables after migrating a cube
 * <p/>
 */
public class CubeMigrationSupplementCLI {

    private static final Logger logger = LoggerFactory.getLogger(CubeMigrationSupplementCLI.class);

    private static KylinConfig srcConfig;
    private static KylinConfig dstConfig;
    private static ResourceStore srcStore;
    private static ResourceStore dstStore;

    public static void main(String[] args) throws InterruptedException, IOException {

        if (args.length != 5) {
            usage();
            System.exit(1);
        }

        moveCubeSupplement(args[0], args[1], args[2], args[3], args[4]);
    }

    private static void usage() {
        System.out.println("Usage: CubeMigrationSupplementCLI srcKylinConfigUri dstKylinConfigUri cubeName projectName realExecute");
        System.out.println(" srcKylinConfigUri: The KylinConfig of the cube’s source \n" + "dstKylinConfigUri: The KylinConfig of the cube’s new home \n" + "cubeName: the name of cube to be migrated. \n" + "projectName: The target project in the target environment.(Make sure it exist) \n" + "realExecute: if false, just print the operations to take, if true, do the real migration. \n");

    }

    public static void moveCubeSupplement(String srcCfgUri, String dstCfgUri, String cubeName, String projectName, String realExecute) throws IOException, InterruptedException {

        moveCubeSupplement(KylinConfig.createInstanceFromUri(srcCfgUri), KylinConfig.createInstanceFromUri(dstCfgUri), cubeName, projectName, realExecute);
    }

    public static void moveCubeSupplement(KylinConfig srcCfg, KylinConfig dstCfg, String cubeName, String projectName, String realExecute) throws IOException, InterruptedException {

        srcConfig = srcCfg;
        srcStore = ResourceStore.getStore(srcConfig);
        dstConfig = dstCfg;
        dstStore = ResourceStore.getStore(dstConfig);

        CubeManager cubeManager = CubeManager.getInstance(srcConfig);
        CubeInstance cube = cubeManager.getCube(cubeName);
        logger.info("cube to be moved is : " + cubeName);

        checkAndGetHbaseUrl();

        List<String> metaItems = new ArrayList<String>();
        listCubeSupplementResources(cube, metaItems);

        if (realExecute.equalsIgnoreCase("true")) {
            copyFilesSupplementInMetaStore(metaItems);
        }else{
            showFilesSupplementInMetaStore(metaItems);
        }
    }

    private static String checkAndGetHbaseUrl() {
        String srcMetadataUrl = srcConfig.getMetadataUrl();
        String dstMetadataUrl = dstConfig.getMetadataUrl();

        logger.info("src metadata url is " + srcMetadataUrl);
        logger.info("dst metadata url is " + dstMetadataUrl);

        int srcIndex = srcMetadataUrl.toLowerCase().indexOf("hbase");
        int dstIndex = dstMetadataUrl.toLowerCase().indexOf("hbase");
        if (srcIndex < 0 || dstIndex < 0)
            throw new IllegalStateException("Both metadata urls should be hbase metadata url");

        String srcHbaseUrl = srcMetadataUrl.substring(srcIndex).trim();
        String dstHbaseUrl = dstMetadataUrl.substring(dstIndex).trim();
        if (!srcHbaseUrl.equalsIgnoreCase(dstHbaseUrl)) {
            throw new IllegalStateException("hbase url not equal! ");
        }

        logger.info("hbase url is " + srcHbaseUrl.trim());
        return srcHbaseUrl.trim();
    }

    private static void copyFilesSupplementInMetaStore(List<String> metaItems) throws IOException {

        for (String item : metaItems) {
            RawResource res = srcStore.getResource(item);
            dstStore.putResource(item, res.inputStream, res.timestamp);
            res.inputStream.close();
            logger.info("Item " + item + " is copied");
        }
    }

    private static void showFilesSupplementInMetaStore(List<String> metaItems) throws IOException {

        for (String item : metaItems) {
            System.out.println("Item " + item + " will be copied");
        }
    }

    private static void listCubeSupplementResources(CubeInstance cube, List<String> metaResource) throws IOException {

        for (CubeSegment segment : cube.getSegments()) {
            metaResource.add(segment.getStatisticsResourcePath());
        }
    }

}