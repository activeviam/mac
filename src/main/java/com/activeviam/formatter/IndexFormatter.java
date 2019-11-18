/*
 * (C) ActiveViam 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.formatter;

import com.qfs.store.impl.MultiVersionCompositePrimaryRecordIndex;
import com.qfs.store.impl.MultiVersionCompositeSecondaryRecordIndex;
import com.qfs.store.impl.MultiVersionPrimaryRecordIndex;
import com.qfs.store.impl.MultiVersionSecondaryRecordIndex;
import com.quartetfs.fwk.QuartetExtendedPluginValue;
import com.quartetfs.fwk.format.IFormatter;
import java.util.HashMap;
import java.util.Map;

/** @author ActiveViam */
@QuartetExtendedPluginValue(intf = IFormatter.class, key = IndexFormatter.KEY)
public class IndexFormatter implements IFormatter {

  private static final long serialVersionUID = 4778274710157958593L;

  /** Plugin key */
  public static final String KEY = "IndexFormatter";

  @Override
  public String getType() {
    return KEY;
  }

  private static final Map<Object, String> membersMap = new HashMap<>();

  static {
    membersMap.put(
        MultiVersionCompositePrimaryRecordIndex.class.getName(), "Composite primary index");
    membersMap.put(
        MultiVersionCompositeSecondaryRecordIndex.class.getName(), "Composite secondary index");
    membersMap.put(MultiVersionPrimaryRecordIndex.class.getName(), "Primary index");
    membersMap.put(MultiVersionSecondaryRecordIndex.class.getName(), "Secondary index");
  }

  @Override
  public String format(final Object object) {
    final String formatted = membersMap.get(object);
    if (formatted == null) {
      return object != null ? object.toString() : null;
    } else {
      return formatted;
    }
  }
}
