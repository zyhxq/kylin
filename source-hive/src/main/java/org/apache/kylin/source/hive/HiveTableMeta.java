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

package org.apache.kylin.source.hive;

import java.util.List;

class HiveTableMeta {
    static class HiveTableColumnMeta {
        String name;
        String dataType;
        String comment;

        public HiveTableColumnMeta(String name, String dataType, String comment) {
            this.name = name;
            this.dataType = dataType;
            this.comment = comment;
        }

        @Override
        public String toString() {
            return "HiveTableColumnMeta{" + "name='" + name + '\'' + ", dataType='" + dataType + '\'' + ", comment='" + comment + '\'' + '}';
        }
    }

    String tableName;
    String sdLocation;//sd is short for storage descriptor
    String sdInputFormat;
    String sdOutputFormat;
    String owner;
    String tableType;
    int lastAccessTime;
    long fileSize;
    long fileNum;
    boolean isNative;
    List<HiveTableColumnMeta> allColumns;
    List<HiveTableColumnMeta> partitionColumns;

    public HiveTableMeta(String tableName, String sdLocation, String sdInputFormat, String sdOutputFormat, String owner, String tableType, int lastAccessTime, long fileSize, long fileNum, boolean isNative, List<HiveTableColumnMeta> allColumns, List<HiveTableColumnMeta> partitionColumns) {
        this.tableName = tableName;
        this.sdLocation = sdLocation;
        this.sdInputFormat = sdInputFormat;
        this.sdOutputFormat = sdOutputFormat;
        this.owner = owner;
        this.tableType = tableType;
        this.lastAccessTime = lastAccessTime;
        this.fileSize = fileSize;
        this.fileNum = fileNum;
        this.isNative = isNative;
        this.allColumns = allColumns;
        this.partitionColumns = partitionColumns;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSdLocation() {
        return sdLocation;
    }

    public void setSdLocation(String sdLocation) {
        this.sdLocation = sdLocation;
    }

    public String getSdInputFormat() {
        return sdInputFormat;
    }

    public void setSdInputFormat(String sdInputFormat) {
        this.sdInputFormat = sdInputFormat;
    }

    public String getSdOutputFormat() {
        return sdOutputFormat;
    }

    public void setSdOutputFormat(String sdOutputFormat) {
        this.sdOutputFormat = sdOutputFormat;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public int getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(int lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public long getFileNum() {
        return fileNum;
    }

    public void setFileNum(long fileNum) {
        this.fileNum = fileNum;
    }

    public boolean isNative() {
        return isNative;
    }

    public void setNative(boolean aNative) {
        isNative = aNative;
    }

    public List<HiveTableColumnMeta> getAllColumns() {
        return allColumns;
    }

    public void setAllColumns(List<HiveTableColumnMeta> allColumns) {
        this.allColumns = allColumns;
    }

    public List<HiveTableColumnMeta> getPartitionColumns() {
        return partitionColumns;
    }

    public void setPartitionColumns(List<HiveTableColumnMeta> partitionColumns) {
        this.partitionColumns = partitionColumns;
    }

    @Override
    public String toString() {
        return "HiveTableMeta{" + "tableName='" + tableName + '\'' + ", sdLocation='" + sdLocation + '\'' + ", sdInputFormat='" + sdInputFormat + '\'' + ", sdOutputFormat='" + sdOutputFormat + '\'' + ", owner='" + owner + '\'' + ", tableType='" + tableType + '\'' + ", lastAccessTime=" + lastAccessTime + ", fileSize=" + fileSize + ", fileNum=" + fileNum + ", isNative=" + isNative + ", allColumns=" + allColumns + ", partitionColumns=" + partitionColumns + '}';
    }
}
