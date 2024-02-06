/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

/** Enum listing all components being part of an Aggregate Provider. */
public enum ProviderComponentType {

  /** Provider Point Index. */
  POINT_INDEX("PointIndex"),
  /** Provider mapping from Points to Aggregated values. */
  POINT_MAPPING("PointMapping"),
  /** Provider Aggregate Store. */
  AGGREGATE_STORE("AggregateStore"),
  /** Provider Point matcher, using Bitmaps. */
  BITMAP_MATCHER("PointMatcher");

  /** User-friendly name of the Provider component. */
  final String friendlyName;

  /**
   * Full constructor.
   *
   * @param friendlyName user-friendly name of the component.
   */
  ProviderComponentType(String friendlyName) {
    this.friendlyName = friendlyName;
  }

  @Override
  public String toString() {
    return this.friendlyName;
  }
}
