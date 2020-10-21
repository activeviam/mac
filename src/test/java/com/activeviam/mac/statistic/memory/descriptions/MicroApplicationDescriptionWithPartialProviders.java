/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.descriptions;

import com.activeviam.copper.HierarchyIdentifier;
import com.activeviam.pivot.builders.StartBuilding;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.literal.ILiteralType;
import com.qfs.store.IDatastore;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import java.util.stream.IntStream;

public class MicroApplicationDescriptionWithPartialProviders
		implements ITestApplicationDescription {

	public static final int CHUNK_SIZE = 256;

	@Override
	public IDatastoreSchemaDescription datastoreDescription() {
		return StartBuilding.datastoreSchema()
				.withStore(
						StartBuilding.store()
								.withStoreName("A")
								.withField("id", ILiteralType.INT)
								.asKeyField()
								.withField("hierId", ILiteralType.INT)
								.withField("measure1", ILiteralType.DOUBLE)
								.withField("measure2", ILiteralType.DOUBLE)
								.withChunkSize(CHUNK_SIZE)
								.build())
				.build();
	}

	@Override
	public IActivePivotManagerDescription managerDescription(
			IDatastoreSchemaDescription schemaDescription) {
		return StartBuilding.managerDescription()
				.withSchema()
				.withSelection(
						StartBuilding.selection(schemaDescription)
								.fromBaseStore("A")
								.withAllFields()
								.build())
				.withCube(
						StartBuilding.cube("Cube")
								.withContributorsCount()
								.withAggregatedMeasure()
								.sum("measure1")
								.withAggregatedMeasure()
								.sum("measure2")

								.withSingleLevelDimension("id")
								.asDefaultHierarchy()
								.withSingleLevelDimension("hierId")

								.withAggregateProvider()
								.bitmap()
								.withPartialProvider()
								.bitmap()
								.excludingHierarchies(HierarchyIdentifier.simple("hierId"))
								.includingOnlyMeasures("measure1.SUM")

								.withPartialProvider()
								.leaf()
								.includingOnlyMeasures("measure2.SUM")

								.withPartialProvider()
								.bitmap()
								.includingOnlyMeasures("contributors.COUNT")

								.build())
				.build();
	}

	public static void fillWithGenericData(IDatastore datastore, IActivePivotManager manager) {
		datastore.edit(
				tm -> {
					IntStream.range(0, 100)
							.forEach(
									i -> {
										tm.add("A", i, i, i * i, -i * i);
									});
				});
	}
}
