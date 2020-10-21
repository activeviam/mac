/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.descriptions;

import com.activeviam.pivot.builders.StartBuilding;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.literal.ILiteralType;
import com.qfs.store.IDatastore;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.impl.ActivePivotManagerDescription;
import java.util.stream.IntStream;

public class MicroApplicationDescriptionWithIndexedFields implements ITestApplicationDescription {

	public static final int ADDED_DATA_SIZE = 20;
	public static final int CHUNK_SIZE = 256;

	@Override
	public IDatastoreSchemaDescription datastoreDescription() {
		return StartBuilding.datastoreSchema()
				.withStore(
						StartBuilding.store()
								.withStoreName("A")
								.withField("id0", ILiteralType.INT)
								.asKeyField()
								.withField("id1", ILiteralType.INT)
								.asKeyField()
								.withField("id2", ILiteralType.INT)
								.asKeyField()
								.withField("field", ILiteralType.INT)
								.dictionarized()
								.withChunkSize(CHUNK_SIZE)
								.build())
				.withStore(StartBuilding.store()
						.withStoreName("B")
						.withField("id0", ILiteralType.INT)
						.asKeyField()
						.build())
				.withReference(StartBuilding.reference()
						.fromStore("A")
						.toStore("B")
						.withName("ref")
						.withMapping("id0", "id0")
						.build())
				.build();
	}

	@Override
	public IActivePivotManagerDescription managerDescription(
			IDatastoreSchemaDescription schemaDescription) {
		return new ActivePivotManagerDescription();
	}

	@Override
	public void fill(IDatastore datastore) throws DatastoreTransactionException {
		datastore.edit(tm -> IntStream.range(0, ADDED_DATA_SIZE)
				.forEach(i -> tm.add("A", i, i % 11, i % 7, i % 5)));
	}
}
