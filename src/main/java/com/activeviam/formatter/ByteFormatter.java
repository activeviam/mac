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
 * @author Quartet FS
 */
@QuartetExtendedPluginValue(intf = IFormatter.class, key = ByteFormatter.KEY)
public class ByteFormatter implements IFormatter {

	private static final long serialVersionUID = 4778274710157958593L;

	/** Plugin key */
	public static final String KEY = "ByteFormatter";

	/** 1 GiB */
	protected final static long GB = 1 << 30;
	/** 1 MiB */
	protected final static long MB = 1 << 20;
	/** 1 KiB */
	protected final static long KB = 1 << 10;

	@Override
	public String getType() {
		return KEY;
	}

	@Override
	public String format(Object object) {
		return printDataSize((long) object);
	}

	/**
	 *
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
			return pnz(gb, "GiB") + pnz(mb, "MiB");
		} else if (mb > 0) {
			return pnz(mb, "MiB") + pnz(kb, "KiB");
		} else if (kb > 0) {
			return pnz(kb, "KiB") + pnz(remaining, "bytes");
		}
		return pnz(gb, "GiB") + pnz(mb, "MiB") + pnz(kb, "KiB") + pnz(remaining, "bytes");
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

}
