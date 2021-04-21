/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

/** Type of an index. */
public enum IndexType {
  /** Type for an index on a store key field. */
  KEY,
  /** Type for a primary index of a store. */
  UNIQUE,
  /** Type for a secondary index of a store. */
  SECONDARY
}
