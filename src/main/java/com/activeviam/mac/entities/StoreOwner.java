/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.entities;

import lombok.Getter;
import lombok.Value;

/** Sub-class of owners for Datastore store. */
@Value
public class StoreOwner implements ChunkOwner {

  /** Unique name of the store. */
  @Getter(onMethod = @__({@Override}))
  String name;

  @Override
  public OwnerType getType() {
    return OwnerType.STORE;
  }

  @Override
  public String toString() {
    return getType() + " " + this.name;
  }
}
