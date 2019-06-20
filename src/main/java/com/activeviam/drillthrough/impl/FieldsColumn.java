/*
 * (C) ActiveViam 2013
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.drillthrough.impl;

import java.util.Properties;

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

	/**
	 * Key of the plugin enabling the calculated drillthrough column returning the field of the column
	 */
	public static final String PLUGIN_KEY = "FieldsColumn";

	/**
	 *  Full contructor
	 *  <p>
	 * @param name name of the column.
	 * @param fields fields needed to compute this calculated column.
	 * @param properties additional properties.
	 */
	public FieldsColumn(String name, String fields, Properties properties) {
		super(name, fields, properties);
	}

	@Override
	public String getType() {
		return PLUGIN_KEY;
	}

	@Override
	protected Object evaluate(final Object underlyingField) {
		return underlyingField.toString();
	}
}
