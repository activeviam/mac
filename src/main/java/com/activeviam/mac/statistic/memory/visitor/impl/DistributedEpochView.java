/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Represents the epoch view of a distributed cube.
 *
 * <p>This class holds information about the corresponding distributed cube's id.
 *
 * <p>Two distributed epoch views with the same epoch id originating from different distributed
 * cubes are not considered {@code equals}.
 */
@Data
@AllArgsConstructor
public class DistributedEpochView implements EpochView {

  private String distributedCubeId;
  private long epochId;

  @Override
  public String toString() {
    return "[query cube " + distributedCubeId + "] " + epochId;
  }
}
