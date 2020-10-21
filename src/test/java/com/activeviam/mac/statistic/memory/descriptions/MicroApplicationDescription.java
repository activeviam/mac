/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.descriptions;

import com.activeviam.fwk.ActiveViamRuntimeException;
import com.activeviam.pivot.builders.StartBuilding;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.literal.ILiteralType;
import com.qfs.store.IDatastore;
import com.qfs.store.NoTransactionException;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.impl.ActivePivotManagerBuilder;
import java.util.stream.IntStream;

public class MicroApplicationDescription implements ITestApplicationDescription {

	public static final int CHUNK_SIZE = 256;
	public static final int ADDED_DATA_SIZE = 100;
	public static final int REMOVED_DATA_SIZE = 10;

	@Override
	public IDatastoreSchemaDescription datastoreDescription() {
		return StartBuilding.datastoreSchema()
				.withStore(
						StartBuilding.store()
								.withStoreName("A")
								.withField("id", ILiteralType.INT)
								.asKeyField()
								.withChunkSize(CHUNK_SIZE)
								.build())
				.build();
	}

	@Override
	public IActivePivotManagerDescription managerDescription(
			IDatastoreSchemaDescription schemaDescription) {
		final IActivePivotManagerDescription managerDescription = StartBuilding.managerDescription()
				.withSchema()
				.withSelection(
						StartBuilding.selection(schemaDescription)
								.fromBaseStore("A")
								.withAllFields()
								.build())
				.withCube(
						StartBuilding.cube("Cube")
								.withContributorsCount()
								.withSingleLevelDimension("id")
								.asDefaultHierarchy()
								.build())
				.build();

		return ActivePivotManagerBuilder.postProcess(managerDescription, schemaDescription);
	}

	@Override
	public void fill(IDatastore datastore) throws DatastoreTransactionException {
		datastore.edit(
				tm -> {
					IntStream.range(0, ADDED_DATA_SIZE)
							.forEach(
									i -> {
										tm.add("A", i * i);
									});
				});

		datastore.edit(
				tm -> {
					IntStream.range(50, 50 + REMOVED_DATA_SIZE)
							.forEach(
									i -> {
										try {
											tm.remove("A", i * i);
										} catch (NoTransactionException
												| DatastoreTransactionException
												| IllegalArgumentException
												| NullPointerException e) {
											throw new ActiveViamRuntimeException(e);
										}
									});
				});
	}
}
