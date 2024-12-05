/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor;

import static org.assertj.core.api.Assertions.assertThat;

import com.activeviam.mac.comparators.ReverseEpochViewComparator;
import com.activeviam.mac.statistic.memory.visitor.impl.DistributedEpochView;
import com.activeviam.mac.statistic.memory.visitor.impl.EpochView;
import com.activeviam.mac.statistic.memory.visitor.impl.RegularEpochView;
import org.assertj.core.api.Condition;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;

public class TestReverseEpochViewComparator {

  private static Condition<EpochView> greaterThan(final EpochView view) {
    return new Condition<>(
        (actual) -> new ReverseEpochViewComparator().compare(view, actual) > 0,
        "greater than %s",
        view);
  }

  private static Condition<EpochView> lessThan(final EpochView view) {
    return new Condition<>(
        (actual) -> new ReverseEpochViewComparator().compare(view, actual) < 0,
        "less than %s",
        view);
  }

  private static Condition<EpochView> sameAs(final EpochView view) {
    return new Condition<>(
        (actual) -> new ReverseEpochViewComparator().compare(view, actual) == 0,
        "same as %s",
        view);
  }

  @Test
  public void testEqualRegularEpochs() {
    final EpochView epoch1 = new RegularEpochView(1L);
    final EpochView epoch1Bis = new RegularEpochView(1L);
    assertThat(epoch1).is(sameAs(epoch1Bis));
  }

  @Test
  public void testGreaterRegularEpochs() {
    final EpochView epoch1 = new RegularEpochView(1L);
    final EpochView epoch2 = new RegularEpochView(2L);
    assertThat(epoch2).is(greaterThan(epoch1));
  }

  @Test
  public void testLesserRegularEpochs() {
    final EpochView epoch1 = new RegularEpochView(1L);
    final EpochView epoch2 = new RegularEpochView(2L);
    assertThat(epoch1).is(lessThan(epoch2));
  }

  @Test
  public void testEqualDistributedEpochs() {
    final EpochView epoch1 = new DistributedEpochView("", 1L);
    final EpochView epoch2 = new DistributedEpochView("", 1L);
    assertThat(epoch1).is(sameAs(epoch2));
    assertThat(epoch2).is(sameAs(epoch1));
  }

  @Test
  public void testGreaterDistributedEpochs() {
    final EpochView epoch1 = new DistributedEpochView("", 1L);
    final EpochView epoch2 = new DistributedEpochView("", 2L);
    assertThat(epoch2).is(greaterThan(epoch1));
  }

  @Test
  public void testLesserDistributedEpochs() {
    final EpochView epoch2A = new DistributedEpochView("A", 2L);
    final EpochView epoch1A = new DistributedEpochView("A", 1L);
    assertThat(epoch1A).is(lessThan(epoch2A));
  }

  @Test
  public void testLexicographicalDistributedEpochs() {
    final EpochView epoch2A = new DistributedEpochView("A", 2L);
    final EpochView epoch1B = new DistributedEpochView("B", 1L);
    SoftAssertions.assertSoftly(
        assertions -> {
          assertions.assertThat(epoch1B).is(lessThan(epoch2A));
          assertions.assertThat(epoch2A).is(greaterThan(epoch1B));
        });
  }

  @Test
  public void testDistributedEpochGreaterThanRegular() {
    final EpochView epoch2A = new DistributedEpochView("A", 2L);
    final EpochView epoch1 = new RegularEpochView(1L);
    SoftAssertions.assertSoftly(
        assertions -> {
          assertions.assertThat(epoch1).is(lessThan(epoch2A));
          assertions.assertThat(epoch2A).is(greaterThan(epoch1));
        });
  }

  @Test
  public void testEpochDoesAffectComparisonWithDifferentTypes() {
    final EpochView epoch2 = new RegularEpochView(2L);
    final EpochView epoch1A = new DistributedEpochView("A", 1L);
    SoftAssertions.assertSoftly(
        assertions -> {
          assertions.assertThat(epoch1A).is(greaterThan(epoch2));
          assertions.assertThat(epoch2).is(lessThan(epoch1A));
        });
  }
}
