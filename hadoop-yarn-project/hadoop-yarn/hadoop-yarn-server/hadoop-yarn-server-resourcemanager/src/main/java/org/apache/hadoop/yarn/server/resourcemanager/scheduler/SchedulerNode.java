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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeOvercommitConfiguration;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import com.google.common.collect.ImmutableSet;


/**
 * Represents a YARN Cluster Node from the viewpoint of the scheduler.
 */
@Private
@Unstable
public abstract class SchedulerNode {

  private static final Log LOG = LogFactory.getLog(SchedulerNode.class);

  private Resource availableResource = Resource.newInstance(0, 0);
  private Resource usedResource = Resource.newInstance(0, 0);
  private Resource totalResourceCapability;     //node capability
  private Resource nodeTotalResourceCapability; //node capability without overcommit
  private RMContainer reservedContainer;
  private volatile int numContainers;

  private volatile ResourceUtilization containersUtilization =
          ResourceUtilization.newInstance(0, 0, 0f);
  private volatile ResourceUtilization nodeUtilization =
          ResourceUtilization.newInstance(0, 0, 0f);
  private long overcommitIncrementTimestamp = 0;
  private long overcommitPreemptions = 0;

  /* set of containers that are allocated containers */
  private final Map<ContainerId, ContainerInfo> launchedContainers =
      new HashMap<ContainerId, ContainerInfo>();

  private final RMNode rmNode;
  private final String nodeName;
  private volatile boolean overcommitEnabled = false;
  private AtomicBoolean doOvercommitUpdate = new AtomicBoolean(false);
  
  private volatile Set<String> labels = null;
  
  public SchedulerNode(RMNode node, boolean usePortForNodeName,
      Set<String> labels) {
    this.rmNode = node;
    this.availableResource = Resources.clone(node.getTotalCapability());
    this.totalResourceCapability = Resources.clone(node.getTotalCapability());
    this.nodeTotalResourceCapability = Resources.clone(totalResourceCapability);
    if (usePortForNodeName) {
      nodeName = rmNode.getHostName() + ":" + node.getNodeID().getPort();
    } else {
      nodeName = rmNode.getHostName();
    }
    this.labels = ImmutableSet.copyOf(labels);
    updateOvercommitEnable();
  }

  public SchedulerNode(RMNode node, boolean usePortForNodeName) {
    this(node, usePortForNodeName, CommonNodeLabelsManager.EMPTY_STRING_SET);
  }

  public RMNode getRMNode() {
    return this.rmNode;
  }

  /**
   * Set total resources on the node.
   * @param resource total resources on the node.
   */
  public synchronized void setTotalResource(Resource resource){
    if (!resource.equals(this.nodeTotalResourceCapability )) {
      // Adjust overcommit metrics.
      Resource adjustment = Resources.subtract(this.totalResourceCapability,
              this.nodeTotalResourceCapability);
      ClusterMetrics metrics = ClusterMetrics.getMetrics();
      metrics.decrOvercommitMB(adjustment.getMemory());
      metrics.decrOvercommitVirtualCores(adjustment.getVirtualCores());
      // Set both nodeTotalResource and TotalResource to new value
      // The amount to overcommit will be re-calculated on next node
      // heartbeat.
      this.nodeTotalResourceCapability = resource;
      setOvercommitTotalResource(resource);
      if (rmNode.getOvercommitConfiguration().getEnabled()) {
        doOvercommitUpdate.set(true);
      }
    }
  }

  /**
   * Set total resources on the node as part of overcommit
   * @param resource total resources on the node.
   */
  public synchronized void setOvercommitTotalResource(Resource resource) {
      this.totalResourceCapability = resource;
      this.availableResource = Resources.subtract(totalResourceCapability,
              this.usedResource);
  }
  
  /**
   * Get the ID of the node which contains both its hostname and port.
   * 
   * @return the ID of the node
   */
  public NodeId getNodeID() {
    return this.rmNode.getNodeID();
  }

  public String getHttpAddress() {
    return this.rmNode.getHttpAddress();
  }

  /**
   * Get the name of the node for scheduling matching decisions.
   * <p>
   * Typically this is the 'hostname' reported by the node, but it could be
   * configured to be 'hostname:port' reported by the node via the
   * {@link YarnConfiguration#RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME} constant.
   * The main usecase of this is Yarn minicluster to be able to differentiate
   * node manager instances by their port number.
   * 
   * @return name of the node for scheduling matching decisions.
   */
  public String getNodeName() {
    return nodeName;
  }

  /**
   * Get rackname.
   * 
   * @return rackname
   */
  public String getRackName() {
    return this.rmNode.getRackName();
  }

  /**
   * The Scheduler has allocated containers on this node to the given
   * application.
   * 
   * @param rmContainer
   *          allocated container
   */
  public void allocateContainer(RMContainer rmContainer) {
    allocateContainer(rmContainer, false);
  }

  private synchronized void allocateContainer(RMContainer rmContainer,
     boolean launchedOnNode) {
    Container container = rmContainer.getContainer();
    deductAvailableResource(container.getResource());
    ++numContainers;

    launchedContainers.put(container.getId(),
            new ContainerInfo(rmContainer, launchedOnNode));

    LOG.info("Assigned container " + container.getId() + " of capacity "
        + container.getResource() + " on host " + rmNode.getNodeAddress()
        + ", which has " + numContainers + " containers, "
        + getUsedResource() + " used and " + getAvailableResource()
        + " available after allocation");
  }

  /**
   * Get available resources on the node.
   * 
   * @return available resources on the node
   */
  public synchronized Resource getAvailableResource() {
    return this.availableResource;
  }

  /**
   * Get used resources on the node.
   * 
   * @return used resources on the node
   */
  public synchronized Resource getUsedResource() {
    return this.usedResource;
  }

  /**
   * Get total resources on the node.
   * 
   * @return total resources on the node.
   */
  public synchronized Resource getTotalResource() {
    return this.totalResourceCapability;
  }

  /**
   * Get node capability without overcommit
   *
   * @return total resources on the node.
   */
  public synchronized Resource getNodeTotalResource() {
    return this.nodeTotalResourceCapability;
  }

  /**
   * Get overcommitted resources on the node.
   *
   * @return resources that are overcommitted on the node.
   */
  public synchronized Resource getOvercommittedResource() {
    return Resources.componentwiseMax(
            Resources.subtract(usedResource, nodeTotalResourceCapability),
            Resources.none());
  }

  public synchronized boolean isValidContainer(ContainerId containerId) {
    if (launchedContainers.containsKey(containerId)) {
      return true;
    }
    return false;
  }

  private synchronized void updateResource(Container container) {
    addAvailableResource(container.getResource());
    --numContainers;
  }

  /**
   * Release an allocated container on this node.
   * 
   * @param containerId
   * @param releasedByNode whether the release originates from a node update
   *          container to be released
   */
  public synchronized void releaseContainer(ContainerId containerId,
      boolean releasedByNode) {
    ContainerInfo info = launchedContainers.get(containerId);
    if (info == null) {
      return;
    }

    if (!releasedByNode && info.launchedOnNode) {
      // wait until node reports container has completed
      return;
    }

    launchedContainers.remove(containerId);
    Container container = info.container.getContainer();
    updateResource(container);

    doOvercommitUpdate.set(true);
    if (rmNode.getOvercommitConfiguration().getContainerChangeAllowsIncrement()) {
      overcommitIncrementTimestamp = 0;
    }

    LOG.info("Released container " + container.getId() + " of capacity "
        + container.getResource() + " on host " + rmNode.getNodeAddress()
        + ", which currently has " + numContainers + " containers, "
        + getUsedResource() + " used and " + getAvailableResource()
        + " available" + ", release resources=" + true);
  }

  public synchronized void containerStarted(ContainerId containerId) {
    ContainerInfo info = launchedContainers.get(containerId);
    if (info != null) {
      info.launchedOnNode = true;
    }

    // throttle overcommit changes based on containers starting on the node
    doOvercommitUpdate.set(true);
    if (rmNode.getOvercommitConfiguration().getContainerChangeAllowsIncrement()) {
      overcommitIncrementTimestamp = 0;
    }
  }

  private synchronized void addAvailableResource(Resource resource) {
    if (resource == null) {
      LOG.error("Invalid resource addition of null resource for "
          + rmNode.getNodeAddress());
      return;
    }
    Resources.addTo(availableResource, resource);
    Resources.subtractFrom(usedResource, resource);
  }

  private synchronized void deductAvailableResource(Resource resource) {
    if (resource == null) {
      LOG.error("Invalid deduction of null resource for "
          + rmNode.getNodeAddress());
      return;
    }
    Resources.subtractFrom(availableResource, resource);
    Resources.addTo(usedResource, resource);
  }

  /**
   * Reserve container for the attempt on this node.
   */
  public abstract void reserveResource(SchedulerApplicationAttempt attempt,
      Priority priority, RMContainer container);

  /**
   * Unreserve resources on this node.
   */
  public abstract void unreserveResource(SchedulerApplicationAttempt attempt);

  @Override
  public String toString() {
    return "host: " + rmNode.getNodeAddress() + " #containers="
        + getNumContainers() + " available=" + getAvailableResource()
        + " used=" + getUsedResource();
  }

  /**
   * Get number of active containers on the node.
   * 
   * @return number of active containers on the node
   */
  public int getNumContainers() {
    return numContainers;
  }

  public synchronized List<RMContainer> getRunningContainers() {
    List<RMContainer> result = new ArrayList<RMContainer>(launchedContainers.size());
    for (ContainerInfo info : launchedContainers.values()) {
      result.add(info.container);
    }
    return result;
  }

  public synchronized RMContainer getReservedContainer() {
    return reservedContainer;
  }

  protected synchronized void
      setReservedContainer(RMContainer reservedContainer) {
    this.reservedContainer = reservedContainer;
  }

  public synchronized void recoverContainer(RMContainer rmContainer) {
    if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
      return;
    }
    allocateContainer(rmContainer, true);
  }
  
  public Set<String> getLabels() {
    return labels;
  }
  
  public void updateLabels(Set<String> labels) {
    this.labels = labels;
    updateOvercommitEnable();
  }

  /**
   +   * Set the resource utilization of the containers in the node.
   +   * @param containersUtilization Resource utilization of the containers.
   +   */
  public void setAggregatedContainersUtilization(
      ResourceUtilization containersUtilization) {
    this.containersUtilization = containersUtilization;
  }

  /**
   * Get the resource utilization of the containers in the node.
   * @return Resource utilization of the containers.
   */
  public ResourceUtilization getAggregatedContainersUtilization() {
    return this.containersUtilization;
  }

  /**
   * Set the resource utilization of the node. This includes the containers.
   * @param nodeUtilization Resource utilization of the node.
   */
  public void setNodeUtilization(ResourceUtilization nodeUtilization) {
    this.nodeUtilization = nodeUtilization;
  }

  /**
   * Get the resource utilization of the node.
   * @return Resource utilization of the node.
   */
  public ResourceUtilization getNodeUtilization() {
    return this.nodeUtilization;
  }

  /**
   * Get the number of preemptions on this node due to overcommit
   * @return Number of preempted containers from overcommit
   */
  public synchronized long getOvercommitPreemptions() {
    return this.overcommitPreemptions;
  }

  /**
   * Increment the number of preemptions on this node due to overcommit
   */
  public synchronized void incrOvercommitPreemptions() {
    ++this.overcommitPreemptions;
  }

  private void updateOvercommitEnable() {
    boolean enabled = labels == null || labels.isEmpty();
    if (overcommitEnabled != enabled) {
      overcommitEnabled = enabled;
      doOvercommitUpdate.set(true);
    }
  }
  //TODO: return the desired resource
  public synchronized Resource calculateOvercommit(Resource minAlloc) {
    RMNodeOvercommitConfiguration overcommitConfig =
            rmNode.getOvercommitConfiguration();
    Resource newTotal = nodeTotalResourceCapability;
    if (overcommitEnabled && overcommitConfig.getEnabled()) {
      long now = Time.monotonicNow();
      boolean isIncrPeriodElapsed = (now - overcommitIncrementTimestamp)
              >= overcommitConfig.getIncrementPeriodMsec();
      if (!doOvercommitUpdate.getAndSet(false) && !isIncrPeriodElapsed) {
        return totalResourceCapability;
      }
      float memUtilization = (float)nodeUtilization.getPhysicalMemory()
              / (float)nodeTotalResourceCapability.getMemory();
      float memFactor = getOvercommitFactor(memUtilization);
      int desiredMemTotal = Math.round(newTotal.getMemory() * memFactor);

      Resource rsrv = (reservedContainer != null)
              ? reservedContainer.getReservedResource() : Resources.none();

      // cap overcommit increase by the specified increment or the reservation
      int memCap = usedResource.getMemory() + rsrv.getMemory();
      desiredMemTotal = Math.min(desiredMemTotal, memCap);

      // TODO. In 2.7 getCPU() returns 0-100 representing a percentage of
      // total CPU used on the node. In 2.8 this returns number of cores.
      // See HADOOP-12356. unfortunately number of Vcores is hard to use
      // because we don't know the vcore:core ratio. So, for now we assume
      // 2.7 behavior.
      float vcoreUtilization = nodeUtilization.getCPU() / 100.0f;
      float vcoreFactor = getOvercommitFactor(vcoreUtilization);
      int desiredVcoreTotal = Math.round(
              newTotal.getVirtualCores() * vcoreFactor);
      int vcoreCap = usedResource.getVirtualCores() + rsrv.getVirtualCores();
      desiredVcoreTotal = Math.min(desiredVcoreTotal, vcoreCap);

      // round down to min alloc
      int newMemTotal = ResourceCalculator.roundDown(desiredMemTotal,
              minAlloc.getMemory());
      int newVcoreTotal = ResourceCalculator.roundDown(desiredVcoreTotal,
              minAlloc.getVirtualCores());

      // do not shrink resources below allocated or node's original capability
      newMemTotal = Math.max(newMemTotal, usedResource.getMemory());
      newVcoreTotal = Math.max(newVcoreTotal, usedResource.getVirtualCores());
      newMemTotal = Math.max(newMemTotal,
              nodeTotalResourceCapability.getMemory());
      newVcoreTotal = Math.max(newVcoreTotal,
              nodeTotalResourceCapability.getVirtualCores());

      // don't allow increments faster than the increment period
      final int oldMemTotal = totalResourceCapability.getMemory();
      final int oldVcoreTotal = totalResourceCapability.getVirtualCores();
      if (newMemTotal > oldMemTotal || newVcoreTotal > oldVcoreTotal) {
        if (!isIncrPeriodElapsed) {
          newMemTotal = Math.min(newMemTotal, oldMemTotal);
          newVcoreTotal = Math.min(newVcoreTotal, oldVcoreTotal);
          doOvercommitUpdate.set(true);
        } else {
          overcommitIncrementTimestamp = now;
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Overcommit for " + rmNode.getNodeAddress()
                + " memUtil=" + memUtilization
                + " memFactor=" + memFactor
                + " rsrvMem=" + rsrv.getMemory()
                + " desiredMemTotal=" + desiredMemTotal
                + " oldMemTotal=" + oldMemTotal
                + " newMemTotal=" + newMemTotal
                + " vcoreUtil=" + vcoreUtilization
                + " vcoreFactor=" + vcoreFactor
                + " rsrvVcore=" + rsrv.getVirtualCores()
                + " desiredVcoreTotal=" + desiredVcoreTotal
                + " oldVcoreTotal=" + oldVcoreTotal
                + " newVcoreTotal=" + newVcoreTotal);
      }

      newTotal = Resource.newInstance(newMemTotal, newVcoreTotal);
    }
    return newTotal;
  }

  private float getOvercommitFactor(float utilization) {
    return 2.0f - utilization; // 1.0f + (1.0f - utilization)
  }

  private static class ContainerInfo {
    private RMContainer container;
    private boolean launchedOnNode;

    public ContainerInfo(RMContainer container, boolean launchedOnNode) {
      this.container = container;
      this.launchedOnNode = launchedOnNode;
    }
  }

}
