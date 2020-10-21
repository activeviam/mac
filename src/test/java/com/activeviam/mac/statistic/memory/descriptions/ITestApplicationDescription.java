/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.descriptions;

import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.multiversion.IEpochManagementPolicy;
import com.qfs.multiversion.impl.KeepLastEpochPolicy;
import com.qfs.store.IDatastore;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;

public interface ITestApplicationDescription {

	IDatastoreSchemaDescription datastoreDescription();

	IActivePivotManagerDescription managerDescription(IDatastoreSchemaDescription schemaDescription);

	void fill(IDatastore datastore) throws DatastoreTransactionException;

	default IEpochManagementPolicy epochManagementPolicy() {
		return new KeepLastEpochPolicy();
	}
}
