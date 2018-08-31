/*
Copyright 2018 Cloudera Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package com.cloudera.ps;

import org.apache.hadoop.hive.metastore.api.Table;

import java.util.Map;

public class HiveTableDetails {

  private String database;
  private String tableName;
  private String tableType;
  private String format;
  private String location;
  private Long numFiles;
  private Long numRows;
  private Long rawDataSize;
  private Long totalSize;

  HiveTableDetails(Table table) {
    this.database = table.getDbName();
    this.tableName = table.getTableName();
    this.tableType = table.getTableType();
    this.format = table.getSd().getInputFormat();
    this.location = table.getSd().getLocation();
    Map<String,String> parameters = table.getParameters();
    numFiles = getTableStatistic(parameters, "numFiles");
    numRows = getTableStatistic(parameters, "numRows");
    rawDataSize = getTableStatistic(parameters, "rawDataSize");
    totalSize = getTableStatistic(parameters, "totalSize");
  }

  private Long getTableStatistic(Map<String, String> parameters, String key) {
    if (!parameters.containsKey(key))
      return null;
    return Long.parseLong(parameters.get(key));
  }

  public String getDatabase() {
    return database;
  }

  public String getTableName() {
    return tableName;
  }

  public String getTableType() {
    return tableType;
  }

  public String getFormat() {
    return format;
  }

  public String getLocation() {
    return location;
  }

  public Long getNumFiles() {
    return numFiles;
  }

  public Long getNumRows() {
    return numRows;
  }

  public Long getRawDataSize() {
    return rawDataSize;
  }

  public Long getTotalSize() {
    return totalSize;
  }
}