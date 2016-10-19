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

package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.util.LinuxResourceCalculatorPlugin;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.apache.hadoop.yarn.util.WindowsResourceCalculatorPlugin;

/**
 * Implementation of the node resource monitor. It periodically tracks the
 * resource utilization of the node and reports it to the NM.
 */
public class NodeResourceMonitorImpl extends AbstractService implements
        NodeResourceMonitor {

    /** Logging infrastructure. */
    final static Log LOG = LogFactory
            .getLog(NodeResourceMonitorImpl.class);

    /** Interval to monitor the node resource utilization. */
    private long monitoringInterval;
    /** Thread to monitor the node resource utilization. */
    private MonitoringThread monitoringThread;

    /** Resource calculator. */
    private ResourceCalculatorPlugin resourceCalculatorPlugin;

    /** Current <em>resource utilization</em> of the node. */
    private ResourceUtilization nodeUtilization;
    /** Estimating resource utilization of the node. */
    private ResourceUtilization estimatedUtilization;
    private long eMemAddedUp;
    private long eCPUAddedUp;
    private long eMemMarker;
    private long eCPUMarker;
    /** Real Usage Weight. */
    private Float realUsageWeight;

    /** To know the capacity of node and the allowance from user desire */
    // private long nodePhyMem;
    // private long nodePhyCore;
    // private long nodeConfiguredMem;
    // private long nodeConfiguredCore;

    /**
     * Initialize the node resource monitor.
     */
    public NodeResourceMonitorImpl() {
        super(NodeResourceMonitorImpl.class.getName());

        this.monitoringThread = new MonitoringThread();
    }

    /**
     * Initialize the service with the proper parameters.
     */
    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        this.monitoringInterval =
                conf.getLong(YarnConfiguration.NM_RESOURCE_MON_INTERVAL_MS,
                        YarnConfiguration.DEFAULT_NM_RESOURCE_MON_INTERVAL_MS);

        Class<? extends ResourceCalculatorPlugin> clazz =
                conf.getClass(YarnConfiguration.NM_MON_RESOURCE_CALCULATOR, null,
                        ResourceCalculatorPlugin.class);

        this.resourceCalculatorPlugin =
                ResourceCalculatorPlugin.getResourceCalculatorPlugin(clazz, conf);

        LOG.info(" Using ResourceCalculatorPlugin : "
                + this.resourceCalculatorPlugin);

        this.realUsageWeight =
                conf.getFloat(YarnConfiguration.NM_RESOURCE_MON_REAL_USAGE_WEIGHT,
                        YarnConfiguration.DEFAULT_NM_RESOURCE_MON_REAL_USAGE_WEIGHT);

       // this.nodePhyMem = resourceCalculatorPlugin.getPhysicalMemorySize();
       // this.nodePhyCore = resourceCalculatorPlugin.getNumProcessors();
       // this.nodeConfiguredMem = conf.getInt(YarnConfiguration.NM_PMEM_MB,
       //         YarnConfiguration.DEFAULT_NM_PMEM_MB);
       // this.nodeConfiguredCore = conf.getInt(YarnConfiguration.NM_VCORES,
       //         YarnConfiguration.DEFAULT_NM_VCORES);

        this.eMemAddedUp = 0L;
        this.eCPUAddedUp = 0L;
        this.eMemMarker = 0L;
        this.eCPUMarker = 0L;
    }

    /**
     * Check if we should be monitoring.
     * @return <em>true</em> if we can monitor the node resource utilization.
     */
    private boolean isEnabled() {
        if (resourceCalculatorPlugin == null) {
            LOG.info("ResourceCalculatorPlugin is unavailable on this system. "
                    + this.getClass().getName() + " is disabled.");
            return false;
        }
        return true;
    }

    /**
     * Start the thread that does the node resource utilization monitoring.
     */
    @Override
    protected void serviceStart() throws Exception {
        if (this.isEnabled()) {
            this.monitoringThread.start();
        }
        super.serviceStart();
    }

    /**
     * Stop the thread that does the node resource utilization monitoring.
     */
    @Override
    protected void serviceStop() throws Exception {
        if (this.isEnabled()) {
            this.monitoringThread.interrupt();
            try {
                this.monitoringThread.join(10 * 1000);
            } catch (InterruptedException e) {
                LOG.warn("Could not wait for the thread to join");
            }
        }
        super.serviceStop();
    }

    /**
     * Thread that monitors the resource utilization of this node.
     */
    private class MonitoringThread extends Thread {
        /**
         * Initialize the node resource monitoring thread.
         */
        public MonitoringThread() {
            super("Node Resource Monitor");
            this.setDaemon(true);
        }

        /**
         * Periodically monitor the resource utilization of the node.
         */
        @Override
        public void run() {
            while (true) {
                // Get node utilization and save it into the health status
                long pmem = resourceCalculatorPlugin.getPhysicalMemorySize() -
                        resourceCalculatorPlugin.getAvailablePhysicalMemorySize();
                pmem = pmem >> 20; // convert from B to MB
                long vmem = resourceCalculatorPlugin.getVirtualMemorySize()
                        - resourceCalculatorPlugin.getAvailableVirtualMemorySize();
                vmem = vmem >> 20; // convert from B to MB
                float cpu = resourceCalculatorPlugin.getCpuUsage();

                // calculate estimated resource usage
                if (isEstimationEnabled()) {
                    long estimatedPmem;
                    long estimatedVmem;
                    float estimatedCpu;
                    if (estimatedUtilization != null) {
                        LOG.info("Last estimated Mem: " + estimatedUtilization.getPhysicalMemory() + " MBs.");
                        estimatedPmem = (long) Math.ceil(estimatedUtilization.getPhysicalMemory()
                                * (1 - realUsageWeight) + pmem * realUsageWeight);
                        estimatedVmem = (long) Math.ceil((estimatedUtilization.getVirtualMemory()
                                * (1 - realUsageWeight)) + (vmem * realUsageWeight));
                        estimatedCpu = (estimatedUtilization.getCPU() * (1 - realUsageWeight))
                                + (cpu * realUsageWeight);

                        // estimated size is a buffer, so cannot get lower than the real usage
                        if (estimatedPmem < pmem) {
                            LOG.info("Reset Estimated Pmem because: epmem: " + estimatedPmem
                                    + " MBs < pmem: " + pmem + " MBs.");
                            estimatedPmem = pmem;
                        }
                        if (estimatedVmem < vmem) {
                            estimatedVmem = vmem;
                        }
                        if (estimatedCpu < cpu) {
                            estimatedCpu = cpu;
                        }

                    } else { // at initializing moment, should be called only once
                        LOG.info("Initializing estimated value with epmem " + pmem
                                + " MBs, ecpu: " + cpu + "%.");
                        estimatedPmem = pmem;
                        estimatedVmem = vmem;
                        estimatedCpu = cpu;
                    }

                    // New container is register during the previous period
                    // adding the demand to the estimation
                    if (eMemAddedUp > eMemMarker) {
                        estimatedPmem = estimatedPmem + (eMemAddedUp - eMemMarker);
                        LOG.info("Add demand to estimation: " + (eMemAddedUp - eMemMarker) + " MBs.");
                        eMemMarker = eMemAddedUp; // update the marker
                    }
                    if (eCPUAddedUp > eCPUMarker) {
                        estimatedCpu = estimatedCpu + (eCPUAddedUp - eCPUMarker);
                        eCPUMarker = eCPUAddedUp;
                    }

                    estimatedUtilization = ResourceUtilization.newInstance(
                            (int) estimatedPmem,
                            (int) estimatedVmem,
                            estimatedCpu);

                    LOG.info("Resource used: epmem: " + estimatedUtilization.getPhysicalMemory()
                            + "MBs, ecpu: " + estimatedUtilization.getCPU() + "%.");

                    //TODO: prevent overload the ADDED_UP variables
                    //TODO: remember to reset the marker also
                }
                // update current real usage
                nodeUtilization =
                        ResourceUtilization.newInstance(
                                (int) pmem,
                                (int) vmem,
                                cpu); // percent of whole node

                // Debug wrong measurement
                LOG.info("Resource used: pmem: " + nodeUtilization.getPhysicalMemory()
                        + "MBs, cpu: " + nodeUtilization.getCPU() + "%.");

                try {
                    Thread.sleep(monitoringInterval);
                } catch (InterruptedException e) {
                    LOG.warn(NodeResourceMonitorImpl.class.getName()
                            + " is interrupted. Exiting.");
                    break;
                }
            }
        }
    }

    /**
     * Get the <em>resource utilization</em> of the node.
     * If weight factor is not the default value, return estimated value.
     * @return <em>resource utilization</em> of the node.
     */
    //@Override
    public ResourceUtilization getUtilization() {
        if (isEstimationEnabled()) {
            return this.estimatedUtilization;
        } else
            return this.nodeUtilization;
    }

    /**
     * Update the estimated resource when there is new container run on the node
     * @param cMem: size of memory container request (MB)
     * @param cCpu: number of core container request
     */
    public void registerNewDemandForEstimatedUtilization(int cMem, int cCpu) {
        if (isEstimationEnabled()) {
            int nCores = resourceCalculatorPlugin.getNumProcessors();
            float pCpu = (((float) cCpu) / nCores) * 100; // in percent of node
            this.eMemAddedUp += cMem;
            this.eCPUAddedUp += pCpu;
            LOG.info("Registering demand to estimation: Cmem:" + cMem + "MB, pCPU: " + pCpu + "%.");
        }
    }

    public boolean isEstimationEnabled() {
        return realUsageWeight < 1 && realUsageWeight >= 0;
    }

    public ResourceUtilization getUsableResource() {
        if (isEstimationEnabled()) {
            return this.estimatedUtilization;
        } else {
            return this.nodeUtilization;
        }
    }
}
