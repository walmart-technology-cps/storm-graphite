/*
 * Copyright 2014 VeriSign, Inc.
 *
 * VeriSign licenses this file to you under the Apache License, version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 */

package com.walmart.cps.storm.metrics.util;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TagsHelperTest {
  private final String stormId = "Example-Topology";
  private final String rawStormId = String.format("%s-1-2345", stormId);

  @DataProvider(name = "taskInfo")
  public Object[][] generateTaskInfo() {
    Map<String, String> expectedTags = generateExpectedTags(stormId, rawStormId);

    return new Object[][]{
      new Object[]{rawStormId, generateTaskInfo(expectedTags), expectedTags},
      new Object[]{rawStormId, generateTaskInfo(getTagsWithPeriods(stormId, rawStormId)), getExpectedSanitizedTags(stormId, rawStormId)}
    };
  }

  @DataProvider(name = "prefix")
  public Object[][] generatePrefix() {
    String prefixConfig = RandomStringUtils.randomAlphanumeric(10);
    Map<String, String> tags1 = generateExpectedTags(stormId, rawStormId);
    Map<String, String> tags2 = generateExpectedTags(stormId, rawStormId);
    return new Object[][]{
      new Object[]{prefixConfig, tags1, getExpectedPrefix(prefixConfig, tags1)},
      new Object[]{prefixConfig, tags2, getExpectedPrefix(prefixConfig, tags2)}
    };
  }

  @Test(dataProvider = "taskInfo")
  public void convertToTags(String stormId, IMetricsConsumer.TaskInfo taskInfo, Map<String, String> expectedTags) {
    assertThat(TagsHelper.convertToTags(stormId, taskInfo)).isEqualTo(expectedTags);
  }

  @Test(dataProvider = "prefix")
  public void constructPrefix(String prefixConfig, Map<String, String> tags, String expectedPrefix) {
    assertThat(TagsHelper.constructMetricPrefix(prefixConfig, tags)).isEqualTo(expectedPrefix);
  }

  private IMetricsConsumer.TaskInfo generateTaskInfo(Map<String, String> tags) {
    return new IMetricsConsumer.TaskInfo(
      tags.get("srcWorkerHost"),
      Integer.valueOf(tags.get("srcWorkerPort")),
      tags.get("srcComponentId"),
      Integer.valueOf(tags.get("srcTaskId")),
      System.currentTimeMillis(),
      10);
  }

  private String getExpectedPrefix(String prefixConfig, Map<String, String> tags) {
    return String.format("%s.%s.%s.%s.%s.%s",
      prefixConfig,
      tags.get("stormId"),
      tags.get("srcComponentId"),
      tags.get("srcWorkerHost"),
      tags.get("srcWorkerPort"),
      tags.get("srcTaskId")
    );
  }

  private Map<String, String> generateExpectedTags(String stormId, String rawStormId) {
    String testStormComponentID = RandomStringUtils.randomAlphanumeric(20);
    String testStormSrcWorkerHost = RandomStringUtils.randomAlphanumeric(20);
    Integer testStormSrcWorkerPort = 6700;
    Integer testStormSrcTaskId = 3008;

    Map<String, String> tags = new HashMap<>();
    tags.put("rawStormId", rawStormId);
    tags.put("stormId", stormId);
    tags.put("srcComponentId", testStormComponentID);
    tags.put("srcWorkerHost", testStormSrcWorkerHost);
    tags.put("srcWorkerPort", String.valueOf(testStormSrcWorkerPort));
    tags.put("srcTaskId", String.valueOf(testStormSrcTaskId));
    return tags;
  }

  private Map<String, String> getTagsWithPeriods(String stormId, String rawStormId) {
    HashMap<String, String> tags = new HashMap<>();
    tags.put("rawStormId", rawStormId);
    tags.put("stormId", stormId);
    tags.put("srcComponentId", "component.id");
    tags.put("srcWorkerHost", "worker.host");
    tags.put("srcWorkerPort", "6700");
    tags.put("srcTaskId", "25");
    return tags;
  }

  private Map<String, String> getExpectedSanitizedTags(String stormId, String rawStormId) {
    HashMap<String, String> tags = new HashMap<>();
    tags.put("rawStormId", rawStormId);
    tags.put("stormId", stormId);
    tags.put("srcComponentId", "component_id");
    tags.put("srcWorkerHost", "worker_host");
    tags.put("srcWorkerPort", "6700");
    tags.put("srcTaskId", "25");
    return tags;
  }
}
