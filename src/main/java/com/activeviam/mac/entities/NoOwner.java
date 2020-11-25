/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.entities;

/** Special class representing the absence of owner. */
public final class NoOwner implements ChunkOwner {

  private static final NoOwner INSTANCE = new NoOwner();

  /**
   * Gets the unique instance of this class.
   *
   * @return the singleton
   */
  public static NoOwner getInstance() {
    return INSTANCE;
  }

  private NoOwner() {}

  @Override
  public String getName() {
    return "none";
  }

  @Override
  public String getType() {
    return "None";
  }

  @Override
  public String toString() {
    return "No owner";
  }
}
