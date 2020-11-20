/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.comparators;

import com.activeviam.mac.statistic.memory.visitor.impl.DistributedEpochView;
import com.activeviam.mac.statistic.memory.visitor.impl.EpochView;
import com.activeviam.mac.statistic.memory.visitor.impl.RegularEpochView;
import com.quartetfs.fwk.QuartetExtendedPluginValue;
import com.quartetfs.fwk.ordering.IComparator;

/**
 * A comparator for epoch views.
 *
 * <p>The order enforced by this comparator respects the following rules:
 * <ul>
 *   <li>{@link RegularEpochView}s are always less than {@link DistributedEpochView}s
 *   <li>more recent {@link RegularEpochView}s are lesser than older {@link RegularEpochView}
 *   <li>{@link DistributedEpochView}s are ordered lexicographically, considering their distributed
 *   cube ids first, and their epoch ids second (more recent epochs are lesser than older ones)
 * </ul>
 */
@QuartetExtendedPluginValue(intf = IComparator.class, key = EpochViewComparator.PLUGIN_KEY)
public class EpochViewComparator implements IComparator<EpochView> {

  /** The plugin key of the comparator. */
  public static final String PLUGIN_KEY = "EpochViewComparator";

  private static final long serialVersionUID = 7843582714929470073L;

  @Override
  public String getType() {
    return PLUGIN_KEY;
  }

  @Override
  public int compare(EpochView epoch1, EpochView epoch2) {

    final boolean isEpoch1Distributed = isDistributedEpoch(epoch1);
    final boolean isEpoch2Distributed = isDistributedEpoch(epoch2);

    if (isEpoch1Distributed) {
      if (isEpoch2Distributed) {
        final DistributedEpochView distributedEpoch1 = ((DistributedEpochView) epoch1);
        final DistributedEpochView distributedEpoch2 = ((DistributedEpochView) epoch2);

        final int cubeNameComparisonResult = distributedEpoch1.getDistributedCubeId()
            .compareTo(distributedEpoch2.getDistributedCubeId());

        if (cubeNameComparisonResult != 0) {
          return cubeNameComparisonResult;
        }
      } else {
        return 1;
      }
    } else {
      if (isEpoch2Distributed) {
        return -1;
      }
    }

    return Long.compare(epoch2.getEpochId(), epoch1.getEpochId());
  }

  private static boolean isDistributedEpoch(EpochView epoch) {
    return epoch instanceof DistributedEpochView;
  }
}
