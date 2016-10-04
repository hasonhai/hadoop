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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Node usage report.
 */
@Private
@Stable
public class SchedulerNodeReport {
  private final Resource used;
  private final Resource avail;
  private final Resource overcommit;
  private final long overcommitPreemptions;
  private final int num;
  
  public SchedulerNodeReport(SchedulerNode node) {
    this.used = node.getUsedResource();
    this.avail = node.getAvailableResource();
    this.overcommit = node.getOvercommittedResource();
    this.overcommitPreemptions = node.getOvercommitPreemptions();
    this.num = node.getNumContainers();
  }
  
  /**
   * @return the amount of resources currently used by the node.
   */
  public Resource getUsedResource() {
    return used;
  }

  /**
   * @return the amount of resources currently available on the node
   */
  public Resource getAvailableResource() {
    return avail;
  }

  /**
   * @return the amount of resources currently overcommitted on the node
   */
  public Resource getOvercommittedResource() {
    return overcommit;
  }

  /**
   * @return the number of preemptions due to overcommit on this node
   */
  public long getOvercommitPreemptions() {
    return overcommitPreemptions;
  }

  /**
   * @return the number of containers currently running on this node.
   */
  public int getNumContainers() {
    return num;
  }
}
