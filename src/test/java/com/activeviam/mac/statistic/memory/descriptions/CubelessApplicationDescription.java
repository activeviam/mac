/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.descriptions;

import com.qfs.desc.IDatastoreSchemaDescription;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.impl.ActivePivotManagerDescription;

public class CubelessApplicationDescription implements ITestApplicationDescription {

	protected final ITestApplicationDescription underlyingApplication;

	public CubelessApplicationDescription(ITestApplicationDescription underlyingApplication) {
		this.underlyingApplication = underlyingApplication;
	}

	@Override
	public IDatastoreSchemaDescription datastoreDescription() {
		return underlyingApplication.datastoreDescription();
	}

	@Override
	public IActivePivotManagerDescription managerDescription(
			IDatastoreSchemaDescription schemaDescription) {
		return new ActivePivotManagerDescription();
	}
}
