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
package org.smartdata.hdfs.scheduler;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.hdfs.action.HdfsAction;
import org.smartdata.hdfs.action.SmallFileCompactAction;
import org.smartdata.hdfs.action.SmallFileWriteAction;
import org.smartdata.metastore.ActionSchedulerService;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.FileContainerInfo;
import org.smartdata.model.FileInfo;
import org.smartdata.model.LaunchAction;
import org.smartdata.model.action.ScheduleResult;

import java.io.IOException;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SmallFileScheduler extends ActionSchedulerService {
  private MetaStore metaStore;
  // <Container file path, retry number>
  private Map<String, Integer> fileLock;
  // <actionId, container file path>
  private Map<Long, String> containerFileMap;
  // <actionId, file container info map>
  private Map<Long, Map<String, FileContainerInfo>> fileContainerInfoMap;
  public static final Logger LOG = LoggerFactory.getLogger(SmallFileScheduler.class);

  public SmallFileScheduler(SmartContext context, MetaStore metaStore) throws IOException {
    super(context, metaStore);
    this.metaStore = metaStore;
  }

  @Override
  public void init() throws IOException {
    this.fileLock = new ConcurrentHashMap<>();
    this.containerFileMap = new ConcurrentHashMap<>();
    this.fileContainerInfoMap = new ConcurrentHashMap<>();
  }

  private static final List<String> actions = Arrays.asList("write", "read", "compact");

  public List<String> getSupportedActions() {
    return actions;
  }

  public ScheduleResult onSchedule(ActionInfo actionInfo, LaunchAction action) {
    long actionId = actionInfo.getActionId();
    if (actionInfo.getActionName().equals("compact")) {
      try {

        // Check if container file is null
        String containerFilePath = action.getArgs().get("-containerFile");
        if (containerFilePath == null) {
          return ScheduleResult.FAIL;
        } else {
          containerFileMap.put(actionId, containerFilePath);
        }

        // Get file container info of small files
        long offset = 0L;
        String smallFiles = action.getArgs().get(HdfsAction.FILE_PATH);
        if (smallFiles == null) {
          return ScheduleResult.FAIL;
        }
        ArrayList<String> smallFileList = new Gson().fromJson(smallFiles, new ArrayList<String>().getClass());
        int start = Integer.valueOf(action.getArgs().get(SmallFileCompactAction.START));
        int stop = Integer.valueOf(action.getArgs().get(SmallFileCompactAction.STOP));
        Map<String, FileContainerInfo> fileContainerInfo = new HashMap<>();
        smallFileList.clear();
        for (int i = start; i <= stop; i++) {
          String tmp = "/benchmarks/TestDFSIO/io_data/test_io_" + i;
          smallFileList.add(tmp);
        }
        String smallFile = new Gson().toJson(smallFileList);
        Map<String, String> args = new HashMap<>();
        args.put(HdfsAction.FILE_PATH, smallFile);
        args.put(SmallFileCompactAction.CONTAINER_FILE, containerFilePath);
        args.put(SmallFileCompactAction.START, "500");
        args.put(SmallFileCompactAction.STOP, "500");
        action.setArgs(args);
        for (String filePath : smallFileList) {
          FileInfo fileInfo = metaStore.getFile(filePath);
          long fileLen = fileInfo.getLength();
          fileContainerInfo.put(filePath, new FileContainerInfo(containerFilePath, offset, fileLen));
          offset += fileLen;
        }
        fileContainerInfoMap.put(actionId, fileContainerInfo);

        // Check if container file is locked and retry
        if (fileLock.containsKey(containerFilePath)) {
          int retryNum = fileLock.get(containerFilePath);
          if (retryNum > 3) {
            LOG.error("This container file: " + containerFilePath + " is locked, failed.");
            return ScheduleResult.FAIL;
          } else {
            LOG.warn("This container file: " + containerFilePath + " is locked, retrying.");
            fileLock.put(containerFilePath, retryNum + 1);
            return ScheduleResult.RETRY;
          }
        } else {
          fileLock.put(containerFilePath, 0); // Lock this container file
        }

        return ScheduleResult.SUCCESS;
      } catch (Exception e) {
        LOG.error("Exception occurred while processing " + action, e);
        return ScheduleResult.FAIL;
      }
    } else if (actionInfo.getActionName().equals("small_file_write")){
      try {

        // Check if container file is null
        String containerFilePath = action.getArgs().get(SmallFileWriteAction.CONTAINER_FILE);
        if (containerFilePath == null) {
          return ScheduleResult.FAIL;
        } else {
          containerFileMap.put(actionId, containerFilePath);
        }

        Map<String, FileContainerInfo> fileContainerInfo = new HashMap<>();
        String filePath = action.getArgs().get(HdfsAction.FILE_PATH);
        File localFile = new File(action.getArgs().get(SmallFileWriteAction.LOCAL_FILE));
        long fileLen = localFile.length();
        fileContainerInfo.put(filePath, new FileContainerInfo(containerFilePath, 0L, fileLen));

        fileContainerInfoMap.put(actionId, fileContainerInfo);

        // Check if container file is locked and retry
        if (fileLock.containsKey(containerFilePath)) {
          int retryNum = fileLock.get(containerFilePath);
          if (retryNum > 3) {
            LOG.error("This container file: " + containerFilePath + " is locked, failed.");
            return ScheduleResult.FAIL;
          } else {
            LOG.warn("This container file: " + containerFilePath + " is locked, retrying.");
            fileLock.put(containerFilePath, retryNum + 1);
            return ScheduleResult.RETRY;
          }
        } else {
          fileLock.put(containerFilePath, 0); // Lock this container file
        }
        return ScheduleResult.SUCCESS;
      } catch (Exception e) {
        LOG.error("Exception occurred while processing " + action, e);
        return ScheduleResult.FAIL;
      }
    } else if (actionInfo.getActionName().equals("write")) {
      // TODO: scheduler for write
      return ScheduleResult.SUCCESS;
    } else if (actionInfo.getActionName().equals("read")) {
      // TODO: scheduler for read
      return ScheduleResult.SUCCESS;
    } else {
      LOG.error("Not support this action: " + actionInfo.getActionName());
      return ScheduleResult.FAIL;
    }
  }

  public void postSchedule(ActionInfo actionInfo, ScheduleResult result) {
  }

  public void onPreDispatch(LaunchAction action) {
  }

  public boolean onSubmit(ActionInfo actionInfo) {
    return true;
  }

  public void onActionFinished(ActionInfo actionInfo) {
    if (actionInfo.isFinished()) {
      if (actionInfo.isSuccessful()) {
        long actionId = actionInfo.getActionId();
        if (actionInfo.getActionName().equals("compact") || actionInfo.getActionName().equals("small_file_write")) {
          try {
            for (Map.Entry<String, FileContainerInfo> entry : fileContainerInfoMap.get(actionId).entrySet()) {
              metaStore.insertSmallFile(entry.getKey(), entry.getValue());
            }
            String containerFilePath = containerFileMap.get(actionId);
            if (fileLock.containsKey(containerFilePath)) {
              fileLock.remove(containerFilePath); // Remove container file lock
            }
            LOG.info("Update file container info successfully.");
          } catch (MetaStoreException e) {
            LOG.error("Process small file action in metaStore failed!", e);
          }
        }
      }
    }
  }

  @Override
  public void stop() throws IOException {
  }

  @Override
  public void start() throws IOException {
  }
}
