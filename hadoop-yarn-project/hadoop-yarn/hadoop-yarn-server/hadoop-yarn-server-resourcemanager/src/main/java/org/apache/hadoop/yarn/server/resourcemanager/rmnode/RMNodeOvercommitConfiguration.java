package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RMNodeOvercommitConfiguration {
    private static final Logger LOG =
            LoggerFactory.getLogger(RMNodeOvercommitConfiguration.class);
    private boolean enabled;
    private long incrementPeriodMsec;
    private boolean containerChangeAllowsIncrement;

    public RMNodeOvercommitConfiguration(Configuration conf) {
        enabled = conf.getBoolean(YarnConfiguration.RM_OVERCOMMIT_ENABLED,
                YarnConfiguration.DEFAULT_RM_OVERCOMMIT_ENABLED);

        incrementPeriodMsec = getConfInt(conf,
                YarnConfiguration.RM_OVERCOMMIT_INCREMENT_PERIOD_MSEC,
                YarnConfiguration.DEFAULT_RM_OVERCOMMIT_INCREMENT_PERIOD_MSEC,
                0, Integer.MAX_VALUE);

        containerChangeAllowsIncrement = conf.getBoolean(
                YarnConfiguration.RM_OVERCOMMIT_CONTAINER_CHANGE_ALLOWS_INCREMENT,
                YarnConfiguration.DEFAULT_RM_OVERCOMMIT_CONTAINER_CHANGE_ALLOWS_INCREMENT);

        LOG.info("Node overcommit initialized."
                        + " IncrementPeriod={} ContainerChangeAllowsIncr={}",
                        incrementPeriodMsec, containerChangeAllowsIncrement);
    }

    public boolean getEnabled() {
        return enabled;
    }

    public long getIncrementPeriodMsec() {
        return incrementPeriodMsec;
    }

    public boolean getContainerChangeAllowsIncrement() {
        return containerChangeAllowsIncrement;
    }

    private static float getConfFloat(Configuration conf, String confKey,
      float defaultValue, float minValue, float maxValue) {
        float confValue = conf.getFloat(confKey, defaultValue);
        float result = Math.min(Math.max(confValue, minValue), maxValue);
        if (confValue != result) {
            LOG.warn("Invalid value " + confValue + " specified for " + confKey
                    + ", using " + result);
        }
        return result;
    }

    private static int getConfInt(Configuration conf, String confKey,
      int defaultValue, int minValue, int maxValue) {
        int confValue = conf.getInt(confKey, defaultValue);
        int result = Math.min(Math.max(confValue, minValue), maxValue);
        if (confValue != result) {
            LOG.warn("Invalid value " + confValue + " specified for " + confKey
                    + ", using " + result);
        }
        return result;
    }
}