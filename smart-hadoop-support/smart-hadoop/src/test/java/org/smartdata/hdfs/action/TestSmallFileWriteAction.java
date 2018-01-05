/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.hdfs.action;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.hdfs.MiniClusterHarness;

import java.util.HashMap;
import java.util.Map;
import java.io.File;

public class TestSmallFileWriteAction extends MiniClusterHarness {
    private String localFile;

    @Before
    @Override
    public void init() throws Exception {
        super.init();
        localFile = this.getClass().getResource("/testSmallFileWriteInput.txt").getFile();
    }

    @Test
    public void testSmallFileWrite() throws Exception {
        SmallFileWriteAction smallFileWriteAction = new SmallFileWriteAction();
        smallFileWriteAction.setDfsClient(dfsClient);
        smallFileWriteAction.setContext(smartContext);
        Map<String, String> args = new HashMap<>();
        args.put(SmallFileWriteAction.LOCAL_FILE, localFile);
        args.put(HdfsAction.FILE_PATH, "/test/small_files/");
        args.put(SmallFileCompactAction.CONTAINER_FILE, "/test/small_files/container_file");
        smallFileWriteAction.init(args);
        smallFileWriteAction.execute();
        File file= new File(localFile);
        long sumFileLen = file.length();
        Assert.assertTrue(dfsClient.exists("/test/small_files/testSmallFileWriteInput.txt"));
        Assert.assertTrue(dfsClient.exists("/test/small_files/container_file"));
        Assert.assertEquals(dfsClient.open("/test/small_files/container_file").getFileLength(), sumFileLen);
    }

}
