/*
 * (C) Quartet FS 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.qfs.formatter;

import java.util.HashMap;
import java.util.Map;

import com.qfs.monitoring.statistic.memory.impl.ChunkSetStatistic;
import com.quartetfs.fwk.QuartetExtendedPluginValue;
import com.quartetfs.fwk.format.IFormatter;

/**
 * @author Quartet FS
 */
@QuartetExtendedPluginValue(intf = IFormatter.class, key = TypeChunkFormatter.KEY)
public class TypeChunkFormatter implements IFormatter {

	private static final long serialVersionUID = 4778274710157958593L;

	/** Plugin key */
	public static final String KEY = "TypeChunkFormatter";

	@Override
	public String getType() {
		return KEY;
	}

	protected static final Map<String, String> membersMap = new HashMap<>();
	static {
		membersMap.put("record", "Record");
		membersMap.put("reference", "Reference");
		membersMap.put("index", "Index");
		membersMap.put(ChunkSetStatistic.QFS_VERSION, "Qfs version");
		membersMap.put("dictionary", "Dictionary");
	}

	@Override
	public String format(Object object) {
		String formatted = membersMap.get(object);
		if (formatted == null) {
			return object != null ? object.toString() : null;
		}

		return formatted;
	}

}
