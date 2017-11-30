/*
 * (C) Quartet FS 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.qfs.formatter;

import java.util.HashMap;
import java.util.Map;

import com.qfs.store.impl.MultiVersionCompositePrimaryRecordIndex;
import com.qfs.store.impl.MultiVersionCompositeSecondaryRecordIndex;
import com.qfs.store.impl.MultiVersionPrimaryRecordIndex;
import com.qfs.store.impl.MultiVersionSecondaryRecordIndex;
import com.quartetfs.fwk.QuartetExtendedPluginValue;
import com.quartetfs.fwk.format.IFormatter;

/**
 * @author Quartet FS
 */
@QuartetExtendedPluginValue(intf = IFormatter.class, key = IndexFormatter.KEY)
public class IndexFormatter implements IFormatter {

	private static final long serialVersionUID = 4778274710157958593L;

	/** Plugin key */
	public static final String KEY = "IndexFormatter";

	@Override
	public String getType() {
		return KEY;
	}

	protected static final Map<String, String> membersMap = new HashMap<>();
	static {
		membersMap.put(MultiVersionCompositePrimaryRecordIndex.class.getName(), "Composite primary index");
		membersMap.put(MultiVersionCompositeSecondaryRecordIndex.class.getName(), "Composite secondary index");
		membersMap.put(MultiVersionPrimaryRecordIndex.class.getName(), "Primary index");
		membersMap.put(MultiVersionSecondaryRecordIndex.class.getName(), "Secondary index");
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
