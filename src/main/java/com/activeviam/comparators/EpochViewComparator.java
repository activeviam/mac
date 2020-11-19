/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.comparators;

import com.quartetfs.fwk.QuartetExtendedPluginValue;
import com.quartetfs.fwk.ordering.IComparator;

@QuartetExtendedPluginValue(intf = IComparator.class, key = EpochViewComparator.PLUGIN_KEY)
public class EpochViewComparator implements IComparator<String> {

  public static final String PLUGIN_KEY = "EpochViewComparator";

  public static final String DISTRIBUTED_EPOCH_PREFIX = "[DISTRIBUTED] ";

  @Override
  public String getType() {
    return PLUGIN_KEY;
  }

  @Override
  public int compare(String epoch1, String epoch2) {
    final boolean isEpoch1Distributed = isDistributedEpoch(epoch1);
    final boolean isEpoch2Distributed = isDistributedEpoch(epoch2);

    if (isEpoch1Distributed) {
      if (isEpoch2Distributed) {
        return Long.compare(parseDistributedEpoch(epoch2), parseDistributedEpoch(epoch1));
      } else {
        return 1;
      }
    } else {
      if (isEpoch2Distributed) {
        return -1;
      } else {
        return Long.compare(parseNormalEpoch(epoch2), parseNormalEpoch(epoch1));
      }
    }
  }

  private static boolean isDistributedEpoch(String epoch) {
    return epoch.startsWith(DISTRIBUTED_EPOCH_PREFIX);
  }

  private static long parseNormalEpoch(String epoch) {
    return Long.parseLong(epoch);
  }

  private static long parseDistributedEpoch(String epoch) {
    return parseNormalEpoch(epoch.substring(DISTRIBUTED_EPOCH_PREFIX.length()));
  }

  // todo vlg: belongs elsewhere? have a class for representing epoch views
  public static String distributedEpochView(long epochId) {
    return DISTRIBUTED_EPOCH_PREFIX + epochId;
  }

  public static String normalEpochView(long epochId) {
    return String.valueOf(epochId);
  }
}
