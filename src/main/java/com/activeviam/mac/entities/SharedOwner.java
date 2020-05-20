/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.entities;

/** Special class symbolizing a owner that owns chunks shared by multiple components. */
public final class SharedOwner implements ChunkOwner {

  private static final SharedOwner INSTANCE = new SharedOwner();

  /**
   * Get the unique instance of this class.
   *
   * @return the singleton
   */
  public static SharedOwner getInstance() {
    return INSTANCE;
  }

  private SharedOwner() {}

  @Override
  public String getName() {
    return "shared";
  }

  @Override
  public String toString() {
    return "Shared";
  }
}
