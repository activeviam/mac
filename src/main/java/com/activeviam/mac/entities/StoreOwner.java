/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.entities;

import lombok.Data;
import lombok.Getter;

/** Sub-class of owners for Datastore store. */
@Data
public class StoreOwner implements ChunkOwner {

  /** Unique name of the store. */
  @Getter(onMethod = @__({@Override}))
  private final String name;

  @Override
  public String getType() {
    return "Store";
  }

  @Override
  public String toString() {
    return "Store " + this.name;
  }
}
