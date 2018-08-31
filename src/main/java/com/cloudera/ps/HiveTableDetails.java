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