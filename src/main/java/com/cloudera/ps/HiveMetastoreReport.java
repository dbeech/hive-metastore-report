package com.cloudera.ps;

import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.PrintWriter;
import java.security.PrivilegedExceptionAction;

public class HiveMetastoreReport {

  private HiveMetastoreReport() {
    //
  }

  private void generate() {
    HiveMetaStoreClient metastore = null;
    PrintWriter writer = new PrintWriter(System.out);
    StatefulBeanToCsv<HiveTableDetails> csvWriter = new StatefulBeanToCsvBuilder<HiveTableDetails>(writer).build();
    try {
      metastore = new HiveMetaStoreClient(new HiveConf());
      for (String databaseName : metastore.getAllDatabases()) {
        for (String tableName : metastore.getAllTables(databaseName)) {
          HiveTableDetails tableDetails = new HiveTableDetails(metastore.getTable(databaseName, tableName));
          csvWriter.write(tableDetails);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      writer.close();
      if (metastore != null) {
        metastore.close();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    final HiveMetastoreReport metastoreReport = new HiveMetastoreReport();
    if ("kerberos".equalsIgnoreCase(conf.get("hadoop.security.authentication"))) {
      String principal = args[0];
      String keytab = args[1];
      UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
      ugi.doAs(new PrivilegedExceptionAction<Object>() {
        public Object run() {
          metastoreReport.generate();
          return null;
        }
      });
      ugi.logoutUserFromKeytab();
    } else {
      metastoreReport.generate();
    }
  }

}
