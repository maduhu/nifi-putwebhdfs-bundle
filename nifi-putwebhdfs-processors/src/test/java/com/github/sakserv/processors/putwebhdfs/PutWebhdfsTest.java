/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.sakserv.processors.putwebhdfs;

import com.github.sakserv.minicluster.impl.HdfsLocalCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;


public class PutWebhdfsTest {

    private TestRunner testRunner;

    // HDFS Mini Cluster Configuration
    private static HdfsLocalCluster hdfsLocalCluster;
    private static final int HDFS_NAMENODE_PORT = 50070;
    private static final int HDFS_NUM_DATANODES = 1;
    private static final String HDFS_TEMP_DIR = "embedded_hdfs";
    private static final boolean HDFS_FORMAT = true;
    private static final boolean HDFS_ENABLE_RUNNING_USER_AS_PROXY_USER = false;
    private static final boolean HDFS_ENABLE_PERMISSIONS = true;
    private static final Configuration HDFS_CONFIGURATION = new Configuration();

    // PutWebhdfs Test Data and Properties
    private static final String TEST_DATA = "test string from file";
    private static final String TEST_BASE_URL = "http://localhost:" + HDFS_NAMENODE_PORT + "/api/v1";
    private static final String TEST_OUTPUT_DIRECTORY = "/tmp";
    private static final String TEST_USER = "hdfs";

    @BeforeClass
    public static void setUp() throws Exception {
        hdfsLocalCluster = new HdfsLocalCluster.Builder()
                .setHdfsNamenodePort(HDFS_NAMENODE_PORT)
                .setHdfsNumDatanodes(HDFS_NUM_DATANODES)
                .setHdfsTempDir(HDFS_TEMP_DIR)
                .setHdfsFormat(HDFS_FORMAT)
                .setHdfsEnableRunningUserAsProxyUser(HDFS_ENABLE_RUNNING_USER_AS_PROXY_USER)
                .setHdfsEnablePermissions(HDFS_ENABLE_PERMISSIONS)
                .setHdfsConfig(HDFS_CONFIGURATION)
                .build();
        hdfsLocalCluster.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        hdfsLocalCluster.stop();
    }

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(PutWebhdfs.class);
    }

    @Test
    public void testProcessor() {

        InputStream content = new ByteArrayInputStream(TEST_DATA.getBytes());

        testRunner.setProperty(PutWebhdfs.WEBHDFS_BASE_URL, TEST_BASE_URL);
        testRunner.setProperty(PutWebhdfs.WEBHDFS_OUTPUT_DIRECTORY, TEST_OUTPUT_DIRECTORY);
        testRunner.setProperty(PutWebhdfs.WEBHDFS_USER, TEST_USER);

        testRunner.enqueue(content);

        testRunner.run(1);

        testRunner.assertQueueEmpty();

    }

}
