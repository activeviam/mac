/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.comparators;

import com.activeviam.mac.statistic.memory.visitor.impl.DistributedEpochView;
import com.activeviam.mac.statistic.memory.visitor.impl.EpochView;
import com.activeviam.mac.statistic.memory.visitor.impl.RegularEpochView;
import com.activeviam.tech.core.api.ordering.IComparator;
import com.activeviam.tech.core.api.registry.AtotiExtendedPluginValue;

/**
 * A comparator for epoch views.
 *
 * <p>The order enforced by this comparator respects the following rules:
 *
 * <ul>
 *   <li>{@link RegularEpochView}s are always less than {@link DistributedEpochView}s
 *   <li>more recent {@link RegularEpochView}s are lesser than older {@link RegularEpochView}
 *   <li>{@link DistributedEpochView}s are ordered lexicographically, considering their distributed
 *       cube ids first, and their epoch ids second (more recent epochs are lesser than older ones)
 * </ul>
 */
@AtotiExtendedPluginValue(intf = IComparator.class, key = ReverseEpochViewComparator.PLUGIN_KEY)
public class ReverseEpochViewComparator implements IComparator<EpochView> {

  /** The plugin key of the comparator. */
  public static final String PLUGIN_KEY = "EpochViewComparator";

  private static final long serialVersionUID = 7843582714929470073L;

  private static boolean isDistributedEpoch(final EpochView epoch) {
    return epoch instanceof DistributedEpochView;
  }

  @Override
  public String getType() {
    return PLUGIN_KEY;
  }

  @Override
  public int compare(final EpochView lhe, final EpochView rhe) {
    final boolean isLhDistributed = isDistributedEpoch(lhe);
    final boolean isRhDistributed = isDistributedEpoch(rhe);

    if (isLhDistributed) {
      if (isRhDistributed) {
        final DistributedEpochView distributedEpoch1 = ((DistributedEpochView) lhe);
        final DistributedEpochView distributedEpoch2 = ((DistributedEpochView) rhe);

        final int cubeNameComparisonResult =
            distributedEpoch1
                .getDistributedCubeId()
                .compareTo(distributedEpoch2.getDistributedCubeId());

        if (cubeNameComparisonResult != 0) {
          return cubeNameComparisonResult;
        }
      } else {
        return -1; // lhe always greater
      }
    } else {
      if (isRhDistributed) {
        return 1; // rhe always greater
      }
    }

    return Long.compare(rhe.getEpochId(), lhe.getEpochId());
  }
}
