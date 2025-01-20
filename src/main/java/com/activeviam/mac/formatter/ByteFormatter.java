/*
 * (C) ActiveViam 2016-2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.formatter;

import com.activeviam.tech.core.api.format.IFormatter;
import com.activeviam.tech.core.api.registry.AtotiExtendedPluginValue;

/**
 * Formatter displaying byte amounts with decimal units.
 *
 * @author ActiveViam
 */
@AtotiExtendedPluginValue(intf = IFormatter.class, key = ByteFormatter.KEY)
public class ByteFormatter implements IFormatter {

  /** Plugin key. */
  public static final String KEY = "ByteFormatter";

  /** Number of bytes in 1 GB. */
  protected static final long GB = 1_000_000_000;

  /** Number of bytes in 1 MB. */
  protected static final long MB = 1_000_000;

  /** Number of bytes in 1 kB. */
  protected static final long KB = 1_000;

  private static final long serialVersionUID = 1342335544322063849L;

  /**
   * Format a number of bytes into a readable data size.
   *
   * @param byteCount The amount of bytes to print.
   * @return data size as text
   */
  public static String printDataSize(long byteCount) {
    if (byteCount == 0) {
      return "0 bytes";
    }

    final long gb = byteCount / GB;
    final long mb = (byteCount % GB) / MB;
    final long kb = (byteCount % MB) / KB;
    final long remaining = (byteCount % KB);
    if (gb > 0) {
      return dec(gb, mb, "GB");
    } else if (mb > 0) {
      return dec(mb, kb, "MB");
    } else if (kb > 0) {
      return dec(kb, remaining, "kB");
    }
    return pnz(gb, "GB") + pnz(mb, "MB") + pnz(kb, "kB") + pnz(remaining, "bytes");
  }

  /**
   * Print if non zero.
   *
   * @param value The value to print.
   * @param suffix The suffix to add to the value if non-zero
   * @return The printed value.
   */
  private static String pnz(long value, String suffix) {
    return value != 0 ? value + " " + suffix + " " : "";
  }

  private static String dec(long value, long decimal, String unit) {
    if (decimal == 0) {
      return String.format("%d %s", value, unit);
    } else {
      return String.format("%.3f %s", value + decimal / 1000.f, unit);
    }
  }

  @Override
  public String getType() {
    return KEY;
  }

  @Override
  public String format(Object object) {
    return printDataSize(((Number) object).longValue());
  }
}
