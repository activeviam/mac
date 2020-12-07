/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.entities;

import lombok.Data;

/** Sub-class of owners for ActivePivot cubes. */
@Data
public class CubeOwner implements ChunkOwner {

  /** Unique id of the ActivePivot. */
  private final String id;

  @Override
  public String getName() {
    return this.id;
  }

  @Override
  public String getType() {
    return "Cube";
  }

  @Override
  public String toString() {
    return "Cube " + this.id;
  }
}
