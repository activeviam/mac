/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

/**
 * A interface for classes that hold information about an epoch view.
 *
 * <p>Implementors should override {@link Object#toString()} to control how these epoch views are
 * formatted in MAC.
 */
public interface EpochView {

  /**
   * Retrieves the epoch underlying id of the epoch view.
   *
   * @return the epoch id
   */
  long getEpochId();
}
