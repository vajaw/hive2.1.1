/**
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
package org.apache.hadoop.hive.shims;

import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.hadoop.util.VersionInfo;
import org.apache.log4j.AppenderSkeleton;

import java.util.HashMap;
import java.util.Map;

/**
 * ShimLoader.
 *
 */
public abstract class ShimLoader {
  public static String HADOOP23VERSIONNAME = "0.23";

  private static volatile HadoopShims hadoopShims;
  private static JettyShims jettyShims;
  private static AppenderSkeleton eventCounter;
  private static HadoopThriftAuthBridge hadoopThriftAuthBridge;
  private static SchedulerShim schedulerShim;

  /**
   * The names of the classes for shimming Hadoop for each major version.
   */
  private static final HashMap<String, String> HADOOP_SHIM_CLASSES =
      new HashMap<String, String>();

  static {
    HADOOP_SHIM_CLASSES.put(HADOOP23VERSIONNAME, "org.apache.hadoop.hive.shims.Hadoop23Shims");
  }

  /**
   * The names of the classes for shimming Jetty for each major version of
   * Hadoop.
   */
  private static final HashMap<String, String> JETTY_SHIM_CLASSES =
      new HashMap<String, String>();

  static {
    JETTY_SHIM_CLASSES.put(HADOOP23VERSIONNAME, "org.apache.hadoop.hive.shims.Jetty23Shims");
  }

  /**
   * The names of the classes for shimming Hadoop's event counter
   */
  private static final HashMap<String, String> EVENT_COUNTER_SHIM_CLASSES =
      new HashMap<String, String>();

  static {
    EVENT_COUNTER_SHIM_CLASSES.put(HADOOP23VERSIONNAME, "org.apache.hadoop.log.metrics" +
        ".EventCounter");
  }

  /**
   * The names of the classes for shimming HadoopThriftAuthBridge
   */
  private static final HashMap<String, String> HADOOP_THRIFT_AUTH_BRIDGE_CLASSES =
      new HashMap<String, String>();

  static {
    HADOOP_THRIFT_AUTH_BRIDGE_CLASSES.put(HADOOP23VERSIONNAME,
        "org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge23");
  }


  private static final String SCHEDULER_SHIM_CLASSE =
    "org.apache.hadoop.hive.schshim.FairSchedulerShim";

  /**
   * Factory method to get an instance of HadoopShims based on the
   * version of Hadoop on the classpath.
   */
  public static HadoopShims getHadoopShims() {
    if (hadoopShims == null) {
      synchronized (ShimLoader.class) {
        if (hadoopShims == null) {
          hadoopShims = loadShims(HADOOP_SHIM_CLASSES, HadoopShims.class);
        }
      }
    }
    return hadoopShims;
  }

  /**
   * Factory method to get an instance of JettyShims based on the version
   * of Hadoop on the classpath.
   */
  public static synchronized JettyShims getJettyShims() {
    if (jettyShims == null) {
      jettyShims = loadShims(JETTY_SHIM_CLASSES, JettyShims.class);
    }
    return jettyShims;
  }

  public static synchronized AppenderSkeleton getEventCounter() {
    if (eventCounter == null) {
      eventCounter = loadShims(EVENT_COUNTER_SHIM_CLASSES, AppenderSkeleton.class);
    }
    return eventCounter;
  }

  public static synchronized HadoopThriftAuthBridge getHadoopThriftAuthBridge() {
    if (hadoopThriftAuthBridge == null) {
      hadoopThriftAuthBridge = loadShims(HADOOP_THRIFT_AUTH_BRIDGE_CLASSES,
          HadoopThriftAuthBridge.class);
    }
    return hadoopThriftAuthBridge;
  }

  public static synchronized SchedulerShim getSchedulerShims() {
    if (schedulerShim == null) {
      schedulerShim = createShim(SCHEDULER_SHIM_CLASSE, SchedulerShim.class);
    }
    return schedulerShim;
  }

  private static <T> T loadShims(Map<String, String> classMap, Class<T> xface) {
    String vers = getMajorVersion();
    String className = classMap.get(vers);
    return createShim(className, xface);
  }

  private static <T> T createShim(String className, Class<T> xface) {
    try {
      Class<?> clazz = Class.forName(className);
      return xface.cast(clazz.newInstance());
    } catch (Exception e) {
      throw new RuntimeException("Could not load shims in class " + className, e);
    }
  }

  /**
   在修改pom中的<project.cdh.version>2.1.1-cdh6.3.2</project.cdh.version>之前，报错如下

   WARNING: Use "yarn jar" to launch YARN applications.
   SLF4J: Class path contains multiple SLF4J bindings.
   SLF4J: Found binding in [jar:file:/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/jars/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.class]
   SLF4J: Found binding in [jar:file:/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/jars/log4j-slf4j-impl-2.8.2.jar!/org/slf4j/impl/StaticLoggerBinder.class]
   SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
   SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
   24/01/03 17:26:06 INFO conf.HiveConf: Found configuration file file:/etc/hive/conf.cloudera.hive/hive-site.xml

   Logging initialized using configuration in jar:file:/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/jars/hive-common-2.1.1-cdh6.3.2.jar!/hive-log4j2.properties Async: false
   24/01/03 17:26:08 INFO SessionState:
   Logging initialized using configuration in jar:file:/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/jars/hive-common-2.1.1-cdh6.3.2.jar!/hive-log4j2.properties Async: false
   24/01/03 17:26:09 INFO hive.metastore: Trying to connect to metastore with URI thrift://optimus30a142:9083
   24/01/03 17:26:09 WARN metadata.Hive: Failed to register all functions.
   java.lang.RuntimeException: Unable to instantiate org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient
   at org.apache.hadoop.hive.metastore.MetaStoreUtils.newInstance(MetaStoreUtils.java:1654)
   at org.apache.hadoop.hive.metastore.RetryingMetaStoreClient.<init>(RetryingMetaStoreClient.java:80)
   at org.apache.hadoop.hive.metastore.RetryingMetaStoreClient.getProxy(RetryingMetaStoreClient.java:130)
   at org.apache.hadoop.hive.metastore.RetryingMetaStoreClient.getProxy(RetryingMetaStoreClient.java:101)
   at org.apache.hadoop.hive.ql.metadata.Hive.createMetaStoreClient(Hive.java:3371)
   at org.apache.hadoop.hive.ql.metadata.Hive.getMSC(Hive.java:3410)
   at org.apache.hadoop.hive.ql.metadata.Hive.getMSC(Hive.java:3390)
   at org.apache.hadoop.hive.ql.metadata.Hive.getAllFunctions(Hive.java:3644)
   at org.apache.hadoop.hive.ql.metadata.Hive.reloadFunctions(Hive.java:240)
   at org.apache.hadoop.hive.ql.metadata.Hive.registerAllFunctionsOnce(Hive.java:225)
   at org.apache.hadoop.hive.ql.metadata.Hive.<init>(Hive.java:370)
   at org.apache.hadoop.hive.ql.metadata.Hive.create(Hive.java:314)
   at org.apache.hadoop.hive.ql.metadata.Hive.getInternal(Hive.java:294)
   at org.apache.hadoop.hive.ql.metadata.Hive.get(Hive.java:270)
   at org.apache.hadoop.hive.ql.session.SessionState.start(SessionState.java:558)
   at org.apache.hadoop.hive.ql.session.SessionState.beginStart(SessionState.java:531)
   at org.apache.hadoop.hive.cli.CliDriver.run(CliDriver.java:763)
   at org.apache.hadoop.hive.cli.CliDriver.main(CliDriver.java:699)
   at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
   at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
   at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
   at java.lang.reflect.Method.invoke(Method.java:498)
   at org.apache.hadoop.util.RunJar.run(RunJar.java:313)
   at org.apache.hadoop.util.RunJar.main(RunJar.java:227)
   Caused by: java.lang.reflect.InvocationTargetException
   at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
   at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
   at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
   at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
   at org.apache.hadoop.hive.metastore.MetaStoreUtils.newInstance(MetaStoreUtils.java:1652)
   ... 23 more
   Caused by: java.lang.IllegalArgumentException: Unrecognized Hadoop major version number: 3.0.0-cdh6.3.2
   at org.apache.hadoop.hive.shims.ShimLoader.getMajorVersion(ShimLoader.java:169)
   at org.apache.hadoop.hive.shims.ShimLoader.loadShims(ShimLoader.java:136)
   at org.apache.hadoop.hive.shims.ShimLoader.getHadoopThriftAuthBridge(ShimLoader.java:122)
   at org.apache.hadoop.hive.metastore.HiveMetaStoreClient.open(HiveMetaStoreClient.java:440)
   at org.apache.hadoop.hive.metastore.HiveMetaStoreClient.<init>(HiveMetaStoreClient.java:285)
   at org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient.<init>(SessionHiveMetaStoreClient.java:70)
   ... 28 more
   Exception in thread "main" java.lang.RuntimeException: org.apache.hadoop.hive.ql.metadata.HiveException: java.lang.RuntimeException: Unable to instantiate org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient
   at org.apache.hadoop.hive.ql.session.SessionState.start(SessionState.java:591)
   at org.apache.hadoop.hive.ql.session.SessionState.beginStart(SessionState.java:531)
   at org.apache.hadoop.hive.cli.CliDriver.run(CliDriver.java:763)
   at org.apache.hadoop.hive.cli.CliDriver.main(CliDriver.java:699)
   at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
   at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
   at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
   at java.lang.reflect.Method.invoke(Method.java:498)
   at org.apache.hadoop.util.RunJar.run(RunJar.java:313)
   at org.apache.hadoop.util.RunJar.main(RunJar.java:227)
   Caused by: org.apache.hadoop.hive.ql.metadata.HiveException: java.lang.RuntimeException: Unable to instantiate org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient
   at org.apache.hadoop.hive.ql.metadata.Hive.registerAllFunctionsOnce(Hive.java:230)
   at org.apache.hadoop.hive.ql.metadata.Hive.<init>(Hive.java:370)
   at org.apache.hadoop.hive.ql.metadata.Hive.create(Hive.java:314)
   at org.apache.hadoop.hive.ql.metadata.Hive.getInternal(Hive.java:294)
   at org.apache.hadoop.hive.ql.metadata.Hive.get(Hive.java:270)
   at org.apache.hadoop.hive.ql.session.SessionState.start(SessionState.java:558)
   ... 9 more
   Caused by: java.lang.RuntimeException: Unable to instantiate org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient
   at org.apache.hadoop.hive.metastore.MetaStoreUtils.newInstance(MetaStoreUtils.java:1654)
   at org.apache.hadoop.hive.metastore.RetryingMetaStoreClient.<init>(RetryingMetaStoreClient.java:80)
   at org.apache.hadoop.hive.metastore.RetryingMetaStoreClient.getProxy(RetryingMetaStoreClient.java:130)
   at org.apache.hadoop.hive.metastore.RetryingMetaStoreClient.getProxy(RetryingMetaStoreClient.java:101)
   at org.apache.hadoop.hive.ql.metadata.Hive.createMetaStoreClient(Hive.java:3371)
   at org.apache.hadoop.hive.ql.metadata.Hive.getMSC(Hive.java:3410)
   at org.apache.hadoop.hive.ql.metadata.Hive.getMSC(Hive.java:3390)
   at org.apache.hadoop.hive.ql.metadata.Hive.getAllFunctions(Hive.java:3644)
   at org.apache.hadoop.hive.ql.metadata.Hive.reloadFunctions(Hive.java:240)
   at org.apache.hadoop.hive.ql.metadata.Hive.registerAllFunctionsOnce(Hive.java:225)
   ... 14 more
   Caused by: java.lang.reflect.InvocationTargetException
   at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
   at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
   at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
   at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
   at org.apache.hadoop.hive.metastore.MetaStoreUtils.newInstance(MetaStoreUtils.java:1652)
   ... 23 more
   Caused by: java.lang.IllegalArgumentException: Unrecognized Hadoop major version number: 3.0.0-cdh6.3.2
   at org.apache.hadoop.hive.shims.ShimLoader.getMajorVersion(ShimLoader.java:169)
   at org.apache.hadoop.hive.shims.ShimLoader.loadShims(ShimLoader.java:136)
   at org.apache.hadoop.hive.shims.ShimLoader.getHadoopThriftAuthBridge(ShimLoader.java:122)
   at org.apache.hadoop.hive.metastore.HiveMetaStoreClient.open(HiveMetaStoreClient.java:440)
   at org.apache.hadoop.hive.metastore.HiveMetaStoreClient.<init>(HiveMetaStoreClient.java:285)
   at org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient.<init>(SessionHiveMetaStoreClient.java:70)
   ... 28 more

   */
  /**
   * Return the "major" version of Hadoop currently on the classpath.
   * Releases in the 1.x and 2.x series are mapped to the appropriate
   * 0.x release series, e.g. 1.x is mapped to "0.20S" and 2.x
   * is mapped to "0.23".
   */
  public static String getMajorVersion() {
    String vers = VersionInfo.getVersion();

    String[] parts = vers.split("\\.");
    if (parts.length < 2) {
      throw new RuntimeException("Illegal Hadoop Version: " + vers +
          " (expected A.B.* format)");
    }

    switch (Integer.parseInt(parts[0])) {
    case 2:
      return HADOOP23VERSIONNAME;
    default:
      throw new IllegalArgumentException("Unrecognized Hadoop major version number: " + vers);
    }
  }

  private ShimLoader() {
    // prevent instantiation
  }
}
