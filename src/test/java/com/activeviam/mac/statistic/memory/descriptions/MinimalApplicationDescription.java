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
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.impl.ActivePivotManagerBuilder;
import java.time.LocalDate;
import java.util.Random;
import java.util.stream.IntStream;

public class MinimalApplicationDescription implements ITestApplicationDescription {

	public static final int STORE_PEOPLE_COUNT = 10;
	public static final int STORE_PRODUCT_COUNT = 20;

	@Override
	public IDatastoreSchemaDescription datastoreDescription() {
		return StartBuilding.datastoreSchema()
				.withStore(
						StartBuilding.store()
								.withStoreName("Sales")
								.withField("id", ILiteralType.INT)
								.asKeyField()
								.withField("seller")
								.withField("buyer")
								.withField("date", ILiteralType.LOCAL_DATE)
								.withField("productId", ILiteralType.LONG)
								.withModuloPartitioning(4, "id")
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
								.fromBaseStore("Sales")
								.withAllFields()
								.build())
				.withCube(
						StartBuilding.cube("HistoryCube")
								.withContributorsCount()
								.withDimension("Operations")
								.withHierarchy("Sales")
								.withLevel("Seller")
								.withPropertyName("seller")
								.withLevel("Buyer")
								.withPropertyName("buyer")
								.withEpochDimension()
								.build())
				.withCube(
						StartBuilding.cube("HistoryCubeLeaf")
								.withContributorsCount()
								.withDimension("Operations")
								.withHierarchy("Sales")
								.withLevel("Seller")
								.withPropertyName("seller")
								.withLevel("Buyer")
								.withPropertyName("buyer")
								.withEpochDimension()
								.withAggregateProvider()
								.leaf()
								.build())
				.build();

		return ActivePivotManagerBuilder.postProcess(managerDescription, schemaDescription);
	}

	public static void fillWithGenericData(IDatastore datastore, IActivePivotManager manager) {
		datastore.edit(
				tm -> {
					final Random r = new Random(47605);
					IntStream.range(0, 1000).forEach(
							i -> {
								final int seller = r.nextInt(STORE_PEOPLE_COUNT);
								int buyer;
								do {
									buyer = r.nextInt(STORE_PEOPLE_COUNT);
								} while (buyer == seller);
								tm.add(
										"Sales",
										i,
										String.valueOf(seller),
										String.valueOf(buyer),
										LocalDate.now().plusDays(-r.nextInt(7)),
										(long) r.nextInt(STORE_PRODUCT_COUNT));
							});
				});
	}
}
