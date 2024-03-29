/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

import lombok.Value;

/** Simple implementation for regular epoch views. */
@Value
public class RegularEpochView implements EpochView {

  long epochId;

  @Override
  public String toString() {
    return String.valueOf(this.epochId);
  }
}
