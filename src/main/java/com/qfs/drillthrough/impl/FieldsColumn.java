/*
 * (C) Quartet FS 2013
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.qfs.drillthrough.impl;

import java.util.Properties;

import com.qfs.monitoring.memory.MemoryAnalysisDatastoreDescription.StringArrayObject;
import com.quartetfs.biz.pivot.context.drillthrough.ICalculatedDrillthroughColumn;
import com.quartetfs.biz.pivot.context.drillthrough.impl.ASimpleCalculatedDrillthroughColumn;
import com.quartetfs.fwk.QuartetExtendedPluginValue;

/**
 * To handler {@link StringArrayObject}.
 *
 * @author Quartet FS
 */
@QuartetExtendedPluginValue(intf = ICalculatedDrillthroughColumn.class, key = FieldsColumn.PLUGIN_KEY)
public class FieldsColumn extends ASimpleCalculatedDrillthroughColumn {

	private static final long serialVersionUID = 1L;

	public static final String PLUGIN_KEY = "FieldsColumn";

	public FieldsColumn(String name, String fields, Properties properties) {
		super(name, fields, properties);
	}

	@Override
	public String getType() {
		return PLUGIN_KEY;
	}

	@Override
	protected Object evaluate(Object underlyingField) {
		return ((StringArrayObject) underlyingField).toString();
	}
}
