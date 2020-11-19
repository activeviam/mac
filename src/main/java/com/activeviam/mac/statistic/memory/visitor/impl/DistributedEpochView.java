/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DistributedEpochView implements EpochView {

  private String distributedCubeName;
  private long epochId;

  @Override
  public String toString() {
    return "[query cube " + distributedCubeName + "] " + epochId;
  }
}
