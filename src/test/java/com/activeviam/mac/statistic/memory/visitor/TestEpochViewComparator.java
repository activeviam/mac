/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor;

import com.activeviam.comparators.EpochViewComparator;
import com.activeviam.mac.statistic.memory.visitor.impl.DistributedEpochView;
import com.activeviam.mac.statistic.memory.visitor.impl.EpochView;
import com.activeviam.mac.statistic.memory.visitor.impl.RegularEpochView;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestEpochViewComparator {

  @Test
  public void testEqualRegularEpochs() {
    final EpochViewComparator comparator = new EpochViewComparator();
    final EpochView epoch1 = new RegularEpochView(1L);
    final EpochView epoch2 = new RegularEpochView(1L);

    Assertions.assertThat(comparator.compare(epoch1, epoch2))
        .isZero();
  }

  @Test
  public void testGreaterRegularEpochs() {
    final EpochViewComparator comparator = new EpochViewComparator();
    final EpochView epoch1 = new RegularEpochView(1L);
    final EpochView epoch2 = new RegularEpochView(2L);

    Assertions.assertThat(comparator.compare(epoch1, epoch2))
        .isPositive();
  }

  @Test
  public void testLesserRegularEpochs() {
    final EpochViewComparator comparator = new EpochViewComparator();
    final EpochView epoch1 = new RegularEpochView(2L);
    final EpochView epoch2 = new RegularEpochView(1L);

    Assertions.assertThat(comparator.compare(epoch1, epoch2))
        .isNegative();
  }

  @Test
  public void testEqualDistributedEpochs() {
    final EpochViewComparator comparator = new EpochViewComparator();
    final EpochView epoch1 = new DistributedEpochView("", 1L);
    final EpochView epoch2 = new DistributedEpochView("", 1L);

    Assertions.assertThat(comparator.compare(epoch1, epoch2))
        .isZero();
  }

  @Test
  public void testGreaterDistributedEpochs() {
    final EpochViewComparator comparator = new EpochViewComparator();
    final EpochView epoch1 = new DistributedEpochView("", 1L);
    final EpochView epoch2 = new DistributedEpochView("", 2L);

    Assertions.assertThat(comparator.compare(epoch1, epoch2))
        .isPositive();
  }

  @Test
  public void testLesserDistributedEpochs() {
    final EpochViewComparator comparator = new EpochViewComparator();
    final EpochView epoch1 = new DistributedEpochView("", 2L);
    final EpochView epoch2 = new DistributedEpochView("", 1L);

    Assertions.assertThat(comparator.compare(epoch1, epoch2))
        .isNegative();
  }

  @Test
  public void testLexicographicalDistributedEpochs() {
    final EpochViewComparator comparator = new EpochViewComparator();
    final EpochView epoch1 = new DistributedEpochView("A", 2L);
    final EpochView epoch2 = new DistributedEpochView("B", 1L);

    Assertions.assertThat(comparator.compare(epoch1, epoch2))
        .isNegative();
  }

  @Test
  public void testDistributedEpochGreaterThanRegular() {
    final EpochViewComparator comparator = new EpochViewComparator();
    final EpochView epoch1 = new DistributedEpochView("A", 2L);
    final EpochView epoch2 = new RegularEpochView(1L);

    Assertions.assertThat(comparator.compare(epoch1, epoch2))
        .isPositive();
  }

  @Test
  public void testRegularEpochLesserThanDistributed() {
    final EpochViewComparator comparator = new EpochViewComparator();
    final EpochView epoch1 = new RegularEpochView(2L);
    final EpochView epoch2 = new DistributedEpochView("A", 1L);

    Assertions.assertThat(comparator.compare(epoch1, epoch2))
        .isNegative();
  }
}
