/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.qfs.store.IDatastore;
import com.quartetfs.biz.pivot.IActivePivotManager;
import lombok.Data;

@Data
public class Application {

  private final IDatastore datastore;
  private final IActivePivotManager manager;
}
