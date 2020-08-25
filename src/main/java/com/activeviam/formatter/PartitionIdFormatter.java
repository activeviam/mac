/*
 * (C) ActiveViam 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.formatter;

import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription;
import com.quartetfs.fwk.QuartetExtendedPluginValue;
import com.quartetfs.fwk.format.IFormatter;

/**
 * Formatter for partitions.
 *
 * @author ActiveViam
 */
@QuartetExtendedPluginValue(intf = IFormatter.class, key = PartitionIdFormatter.KEY)
public class PartitionIdFormatter implements IFormatter {

  private static final long serialVersionUID = 1L;
  /** Plugin key. */
  public static final String KEY = "PartitionIdFormatter";

  @Override
  public String getType() {
    return KEY;
  }

  @Override
  public String format(Object object) {
    final int id = (Integer) object;
    switch (id) {
      case MemoryAnalysisDatastoreDescription.NO_PARTITION:
        return "N/A";
      case MemoryAnalysisDatastoreDescription.MANY_PARTITIONS:
        return "shared";
      default:
        return String.valueOf(id);
    }
  }
}
