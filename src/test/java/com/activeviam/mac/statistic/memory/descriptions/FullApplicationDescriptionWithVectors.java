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
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.impl.ActivePivotManagerBuilder;

public class FullApplicationDescriptionWithVectors implements ITestApplicationDescription {

	public static final String VECTOR_STORE_NAME = "vectorStore";

	@Override
	public IDatastoreSchemaDescription datastoreDescription() {
		return StartBuilding.datastoreSchema()
				.withStore(
						StartBuilding.store()
								.withStoreName(VECTOR_STORE_NAME)
								.withField("vectorId", ILiteralType.INT)
								.asKeyField()
								.withVectorField("vectorInt1", ILiteralType.INT)
								.withVectorBlockSize(35)
								.withVectorField("vectorInt2", ILiteralType.INT)
								.withVectorBlockSize(20)
								.withVectorField("vectorLong", ILiteralType.LONG)
								.withVectorBlockSize(30)
								.withChunkSize(16) // Make it easy to fill a complete block
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
								.fromBaseStore(VECTOR_STORE_NAME)
								.withAllFields()
								.build())
				.withCube(
						StartBuilding.cube("C")
								.withAggregatedMeasure()
								.sum("vectorInt1")
								.withAggregatedMeasure()
								.avg("vectorLong")
								.withSingleLevelDimension("Id")
								.withPropertyName("vectorId")
								.withAggregateProvider()
								.leaf()
								.build())
				.build();

		return ActivePivotManagerBuilder.postProcess(managerDescription, schemaDescription);
	}

	@Override
	public void fill(IDatastore datastore) {
		final int nbOfVectors = 10;
		final int vectorSize = 10;

		// 3 vectors of same size with same values (but not copied one from another), v1, v3 of ints and
		// v2 of long
		final int[] v1 = new int[vectorSize];
		final int[] v3 = new int[vectorSize];
		final long[] v2 = new long[vectorSize];
		for (int j = 0; j < vectorSize; j++) {
			v1[j] = j;
			v2[j] = j;
			v3[j] = j;
		}

		// add the same vectors over and over
		datastore.edit(
				tm -> {
					for (int i = 0; i < nbOfVectors; i++) {
						tm.add(VECTOR_STORE_NAME, i, v1, v3, v2);
					}
				});
	}
}
