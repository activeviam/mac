/*
 * (C) ActiveViam 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.formatter;

import com.activeviam.tech.core.api.format.IFormatter;
import com.activeviam.tech.core.api.registry.AtotiExtendedPluginValue;
import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Basic formatter for class names removing class namespaces from inputs.
 *
 * @author ActiveViam
 */
@AtotiExtendedPluginValue(intf = IFormatter.class, key = ClassFormatter.KEY)
public class ClassFormatter implements IFormatter {

  /** Plugin key. */
  public static final String KEY = "ClassFormatter";

  private static final long serialVersionUID = 1L;

  @Override
  public String getType() {
    return KEY;
  }

  @Override
  public String format(Object object) {
    // A String is expected has input.
    if (object instanceof String) {
      /*
       * Input: com.qfs.chunk.impl.ChunkOffsetLong,com.qfs.chunk.direct.impl.DirectChunkBits Output:
       * ChunkOffsetLong,DirectChunkBits
       */
      String[] classes = ((String) object).split(Pattern.quote(","));
      return Arrays.stream(classes)
          .map(className -> className.replaceAll("^.*\\.", ""))
          .collect(Collectors.joining(","));
    } else {
      if (object == null) {
        return null;
      }
      return object.toString();
    }
  }
}
