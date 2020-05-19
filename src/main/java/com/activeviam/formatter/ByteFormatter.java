/*
 * (C) ActiveViam 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.formatter;

import com.quartetfs.fwk.QuartetExtendedPluginValue;
import com.quartetfs.fwk.format.IFormatter;

/**
 * Formatter displaying byte amounts with units.
 *
 * @author ActiveViam
 */
@QuartetExtendedPluginValue(intf = IFormatter.class, key = ByteFormatter.KEY)
public class ByteFormatter implements IFormatter {

	private static final long serialVersionUID = 4778274710157958593L;

	/**
	 * Plugin key.
	 */
	public static final String KEY = "ByteFormatter";

	/**
	 * Number of bytes in 1 GiB.
	 */
	protected static final long GB = 1 << 30;
	/**
	 * Number of bytes in 1 MiB.
	 */
	protected static final long MB = 1 << 20;
	/**
	 * Number of bytes in 1 KiB.
	 */
	protected static final long KB = 1 << 10;

	@Override
	public String getType() {
		return KEY;
	}

	@Override
	public String format(Object object) {
		return printDataSize((long) object);
	}

	/**
	 * Format a number of bytes into a readable data size.
	 *
	 * @param byteCount The amount of bytes to print.
	 * @return data size as text
	 */
	public static String printDataSize(long byteCount) {
		if (byteCount == 0) {
			return null;
		}

		final long gb = byteCount / GB;
		final long mb = (byteCount % GB) / MB;
		final long kb = (byteCount % MB) / KB;
		final long remaining = (byteCount % KB);
		if (gb > 0) {
			return dec(gb, mb, "GiB");
		} else if (mb > 0) {
			return dec(mb, kb, "MiB");
		} else if (kb > 0) {
			return dec(kb, remaining, "KiB");
		}
		return pnz(gb, "GiB") + pnz(mb, "MiB") + pnz(kb, "KiB") + pnz(remaining, "bytes");
	}

	/**
	 * Print if non zero.
	 *
	 * @param value  The value to print.
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
			return String.format("%.3f %s", value + 0.001f * decimal, unit);
		}
	}
}
