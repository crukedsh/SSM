/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.server.engine.rule;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.SmartContext;
import org.smartdata.admin.SmartAdmin;
import org.smartdata.conf.SmartConf;
import org.smartdata.hdfs.client.SmartDFSClient;
import org.smartdata.model.RuleInfo;
import org.smartdata.model.RuleState;
import org.smartdata.server.MiniSmartClusterHarness;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestSmallFileRule extends MiniSmartClusterHarness {

  @Before
  @Override
  public void init() throws Exception {
    //super.init();
    //createTestFiles();
  }

  private void createTestFiles() throws Exception {
    Path path = new Path("/test/small_files/");
    dfs.mkdirs(path);
    for (int i = 0; i < 3; i++) {
      String fileName = "/test/small_files/file_" + i;
      FSDataOutputStream out = dfs.create(new Path(fileName), (short) 1);
      long fileLen;
      fileLen = 10 + (int) (Math.random() * 11);
      byte[] buf = new byte[20];
      Random rb = new Random(2018);
      int bytesRemaining = (int) fileLen;
      while (bytesRemaining > 0) {
        rb.nextBytes(buf);
        int bytesToWrite = (bytesRemaining < buf.length) ? bytesRemaining : buf.length;
        out.write(buf, 0, bytesToWrite);
        bytesRemaining -= bytesToWrite;
      }
      out.close();
    }
  }

  @Test
  public void testRule() throws Exception {
    waitTillSSMExitSafeMode();

    String rule = "file: path matches \"/test/small_files/file*\" and length < 20KB" +
        " | compact -containerFile \"/test/small_files/container_file\"";
    SmartAdmin admin = new SmartAdmin(smartContext.getConf());
    admin.submitRule(rule, RuleState.ACTIVE);

    Thread.sleep(10000);
    List<RuleInfo> ruleInfoList = admin.listRulesInfo();
    for (RuleInfo info : ruleInfoList) {
      System.out.println(info);
    }
    Assert.assertEquals(1, ruleInfoList.size());
  }

  @Test
  public void testCool() throws Exception {
    SmartConf conf  = new SmartConf();
    System.out.println("1111111");
    SmartContext context = new SmartContext(conf);
    System.out.println(conf.get("smart.dfs.namenode.rpcserver"));
    ExecutorService exec = Executors.newFixedThreadPool(30);
    for (int i = 0; i < 30; i ++) {
      System.out.println("2222222");
      exec.submit(new TestPread.Task("/benchmarks/TestDFSIO/io_data/test__" + i, context));
    }
  }

  public static class Task implements Runnable {
    String path;
    SmartContext context;

    Task(String src, SmartContext smartContext) {
      this.path = src;
      this.context = smartContext;
    }

    @Override
    public void run() {
      try {
        System.out.println("333333333333");
        System.out.println();
        SmartDFSClient smartDFSClient = new SmartDFSClient(context.getConf());
        DFSInputStream in = smartDFSClient.open(path);
        byte[] bytes = new byte[10240];
        int res = in.read(bytes);
        System.out.println(res);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
