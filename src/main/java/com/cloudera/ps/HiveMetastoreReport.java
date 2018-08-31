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
      if (args.length < 2) {
        System.err.println("ERROR: Cluster is configured for Kerberos but no principal and keytab provided. " +
            "Expected args: " + HiveMetastoreReport.class.getName() + " [principal] [keytab]");
        System.exit(1);
      }
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
