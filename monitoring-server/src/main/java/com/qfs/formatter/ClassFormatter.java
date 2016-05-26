/*
 * (C) Quartet FS 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.qfs.formatter;

import com.quartetfs.fwk.QuartetExtendedPluginValue;
import com.quartetfs.fwk.format.IFormatter;

/**
 * @author Quartet FS
 */
@QuartetExtendedPluginValue(intf = IFormatter.class, key = ClassFormatter.KEY)
public class ClassFormatter implements IFormatter {

	/** Plugin key */
	public static final String KEY = "ClassFormatter";

	@Override
	public String getType() {
		return KEY;
	}

	@Override
	public String format(Object object) {
		// A String is expected has input.
		if (object instanceof String) {
			/*
			 * Input: com.qfs.chunk.direct.impl.DirectChunkBits
			 * Output: DirectChunkBits
			 */
			String[] paths = ((String) object).split("\\.");
			return paths[paths.length - 1];
		} else {
			if (object == null)
				return null;
			return object.toString();
		}
	}
}
