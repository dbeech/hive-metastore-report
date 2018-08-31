# Hive Metastore Report

Connects to a Hive metastore and generates a CSV report of the Hive databases and tables inside. 

The following attributes are written:

- Database name
- Table name
- Table type (e.g. `EXTERNAL_TABLE` or `MANAGED_TABLE`)
- Input format class (e.g. `org.apache.hadoop.mapred.TextInputFormat`)
- HDFS location
- Select table stats, if present:
  - `numFiles`
  - `numRows`
  - `rawDataSize`
  - `totalSize`
  
## How to build

Note: requires Apache Maven. 

```
git clone https://github.com/dbeech/hive-metastore-report.git
cd hive-metastore-report
mvn clean package
```

The Maven Shade plugin is used to build all dependencies into the executable jar. 

## How to run

```
$ java -cp /etc/hive/conf:hive-metastore-report-1.0-SNAPSHOT.jar com.cloudera.ps.HiveMetastoreReport <kerberos principal> <kerberos keytab file>
```
Hive client configuration files (e.g. core-site.xml, hive-site.xml) must be on the classpath so that the tool can determine the metastore URIs and other configuration. 

The Kerberos principal and path to keytab file are optional arguments and only required if Hive is secured by Kerberos authentication. 
The Hive metastore report class will determine this by reading the parameter `hadoop.security.authentication`.

## Example output

```
"database","format","location","numFiles","numRows","rawDataSize","tableName","tableType","totalSize"
"default","org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat","hdfs://dbeech-test:8020/user/hive/warehouse/test_table_avro","1","100","1000","test_table_avro","MANAGED_TABLE","1000"
"default","org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat","hdfs://dbeech-test:8020/user/hive/warehouse/test_table_avro_external","8","8720","87200","test_table_avro_external","EXTERNAL_TABLE","87200"
"default","org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat","hdfs://dbeech-test:8020/user/hive/warehouse/test_table_parquet","2","1000","2000","test_table_parquet","MANAGED_TABLE","2000"
"default","org.apache.hadoop.mapred.TextInputFormat","hdfs://dbeech-test:8020/user/hive/warehouse/test_table_text","3","300","3000","test_table_text","MANAGED_TABLE","3000"
```
