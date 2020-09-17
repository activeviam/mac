/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.entities;

/** Interface representing an Owner of a chunk in the application. */
public interface ChunkOwner extends Comparable<ChunkOwner> {

  /**
   * Gets the name of the owner.
   *
   * @return the name
   */
  String getName();

  String getType();

  @Override
  default int compareTo(ChunkOwner o) {
    return this.toString().compareTo(o.toString());
  }
}
