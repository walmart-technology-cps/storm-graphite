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

import com.google.common.base.Optional;
import org.apache.storm.metric.api.IMetricsConsumer;

import java.util.HashMap;
import java.util.Map;

public class TagsHelper {
  public static final String GRAPHITE_PREFIX_OPTION = "metrics.graphite.prefix";
  public static final String DEFAULT_PREFIX = "metrics";

  public static String getPrefix(Map<String, Object> conf) {
    return (String) Optional.fromNullable(conf.get(GRAPHITE_PREFIX_OPTION)).or(DEFAULT_PREFIX);
  }

  public static Map<String, String> convertToTags(String stormId, IMetricsConsumer.TaskInfo taskInfo) {
    Map<String, String> tags = new HashMap<>();
    tags.put("srcComponentId", sanitizeTag(taskInfo.srcComponentId));
    tags.put("stormId", sanitizeTag(removeNonce(stormId)));
    tags.put("rawStormId", sanitizeTag(stormId));
    tags.put("srcWorkerHost", sanitizeTag(taskInfo.srcWorkerHost));
    tags.put("srcWorkerPort", String.valueOf(taskInfo.srcWorkerPort));
    tags.put("srcTaskId", String.valueOf(taskInfo.srcTaskId));
    return tags;
  }

  /**
   * Constructs a fully qualified metric prefix.
   *
   * @param tags The information regarding the context in which the data point is supplied
   * @return A fully qualified metric prefix.
   */
  public static String constructMetricPrefix(String prefix, Map<String, String> tags) {
    StringBuilder sb = new StringBuilder();

    if (prefix == null) {
      throw new IllegalArgumentException("Prefix is required");
    }

    appendTagIfNotNullOrEmpty(sb, prefix);
    appendTagIfNotNullOrEmpty(sb, tags, "stormId");
    appendTagIfNotNullOrEmpty(sb, tags, "srcComponentId");
    appendTagIfNotNullOrEmpty(sb, tags, "srcWorkerHost");
    appendTagIfNotNullOrEmpty(sb, tags, "srcWorkerPort");
    appendTagIfNotNullOrEmpty(sb, tags, "srcTaskId");

    return sb.substring(0, sb.length() - 1);
  }

  /**
   * Removes nonce appended to topology name (e.g. "Example-Topology-1-2345" -> "Example-Topology")
   */
  private static String removeNonce(String topologyId) {
    return topologyId.substring(0, topologyId.substring(0, topologyId.lastIndexOf("-")).lastIndexOf("-"));
  }

  private static void appendTagIfNotNullOrEmpty(StringBuilder sb, Map<String, String> tags, String key) {
    String value = tags.get(key);
    appendTagIfNotNullOrEmpty(sb, value);
  }

  private static void appendTagIfNotNullOrEmpty(StringBuilder sb, String value) {
    if (value != null && !value.isEmpty()) {
      sb.append(value).append(".");
    }
  }

  /**
   * Sanitizes the tags of "." and replaces it with "_" for simpler graphite metric names
   */
  private static String sanitizeTag(String tag) {
    return tag.replace(".", "_");
  }
}
