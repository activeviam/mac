/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.comparators;

import com.quartetfs.fwk.QuartetExtendedPluginValue;
import com.quartetfs.fwk.ordering.IComparator;
import java.text.MessageFormat;

@QuartetExtendedPluginValue(intf = IComparator.class, key = EpochViewComparator.PLUGIN_KEY)
public class EpochViewComparator implements IComparator<String> {

  public static final String PLUGIN_KEY = "EpochViewComparator";

  private static final String DISTRIBUTED_EPOCH_PREFIX = "[Query Cube ";
  private static final String DISTRIBUTED_EPOCH_FULL_PREFIX = DISTRIBUTED_EPOCH_PREFIX + "{0}] ";

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
    return parseNormalEpoch(epoch.substring(
        epoch.indexOf(']', DISTRIBUTED_EPOCH_PREFIX.length()) + 2));
  }

  // todo vlg: belongs elsewhere? have a class for representing epoch views
  public static String distributedEpochView(String ownerName, long epochId) {
    return MessageFormat.format(DISTRIBUTED_EPOCH_FULL_PREFIX, ownerName) + epochId;
  }

  public static String normalEpochView(long epochId) {
    return String.valueOf(epochId);
  }
}
