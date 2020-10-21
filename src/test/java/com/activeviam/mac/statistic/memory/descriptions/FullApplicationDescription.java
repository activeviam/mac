/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.descriptions;

import com.activeviam.builders.FactFilterConditions;
import com.activeviam.pivot.builders.StartBuilding;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.literal.ILiteralType;
import com.qfs.store.IDatastore;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.impl.ActivePivotManagerBuilder;
import java.time.LocalDate;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class FullApplicationDescription implements ITestApplicationDescription {

	public static final int STORE_PEOPLE_COUNT = 10;
	public static final int STORE_PRODUCT_COUNT = 20;
	protected static final AtomicInteger operationsBatch = new AtomicInteger();

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
				.withStore(
						StartBuilding.store()
								.withStoreName("People")
								.withField("id")
								.asKeyField()
								.withField("firstName")
								.withField("lastName")
								.withField("company")
								.build())
				.withStore(
						StartBuilding.store()
								.withStoreName("Products")
								.withField("id", ILiteralType.LONG)
								.asKeyField()
								.withField("name")
								.build())
				.withReference(
						StartBuilding.reference()
								.fromStore("Sales")
								.toStore("People")
								.withName("Sales->Buyer")
								.withMapping("buyer", "id")
								.build())
				.withReference(
						StartBuilding.reference()
								.fromStore("Sales")
								.toStore("People")
								.withName("Sales->Seller")
								.withMapping("seller", "id")
								.build())
				.withReference(
						StartBuilding.reference()
								.fromStore("Sales")
								.toStore("Products")
								.withName("Sales->Products")
								.withMapping("productId", "id")
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
								.usingReference("Sales->Products")
								.withField("product", "name")
								.usingReference("Sales->Buyer")
								.withField("buyer_firstName", "firstName")
								.withField("buyer_lastName", "lastName")
								.withField("buyer_company", "company")
								.usingReference("Sales->Seller")
								.withField("seller_firstName", "firstName")
								.withField("seller_lastName", "lastName")
								.withField("seller_company", "company")
								.build())
				.withCube(
						StartBuilding.cube("HistoryCube")
								.withContributorsCount()
								.withSingleLevelDimension("Product")
								.withPropertyName("product")
								.withSingleLevelDimension("Date")
								.withPropertyName("date")
								.withDimension("Operations")
								.withHierarchy("Sellers")
								.withLevel("Company")
								.withPropertyName("seller_company")
								.withLevel("LastName")
								.withPropertyName("seller_lastName")
								.withLevel("FirstName")
								.withPropertyName("seller_firstName")
								.withHierarchy("Buyers")
								.withLevel("Company")
								.withPropertyName("buyer_company")
								.withLevel("LastName")
								.withPropertyName("buyer_lastName")
								.withLevel("FirstName")
								.withPropertyName("buyer_firstName")
								.withFilter(
										FactFilterConditions.not(FactFilterConditions.eq("date", LocalDate.now())))
								.withEpochDimension()
								.build())
				.withCube(
						StartBuilding.cube("DailyCube")
								.withContributorsCount()
								.withSingleLevelDimension("Product")
								.withPropertyName("product")
								.withDimension("Operations")
								.withHierarchy("Sellers")
								.withLevel("Company")
								.withPropertyName("seller_company")
								.withLevel("LastName")
								.withPropertyName("seller_lastName")
								.withLevel("FirstName")
								.withPropertyName("seller_firstName")
								.withHierarchy("Buyers")
								.withLevel("Company")
								.withPropertyName("buyer_company")
								.withLevel("LastName")
								.withPropertyName("buyer_lastName")
								.withLevel("FirstName")
								.withPropertyName("buyer_firstName")
								.withFilter(FactFilterConditions.eq("date", LocalDate.now()))
								.withEpochDimension()
								.build())
				.withCube(
						StartBuilding.cube("OverviewJITCube")
								.withContributorsCount()
								.withSingleLevelDimension("Product")
								.withPropertyName("product")
								.withSingleLevelDimension("Date")
								.withPropertyName("date")
								.withDimension("Operations")
								.withHierarchy("Sales")
								.withLevel("Seller")
								.withPropertyName("seller_company")
								.withLevel("Buyer")
								.withPropertyName("buyer_company")
								.withHierarchy("Purchases")
								.withLevel("Buyer")
								.withPropertyName("buyer_company")
								.withLevel("Seller")
								.withPropertyName("seller_company")
								.withEpochDimension()
								.withAggregateProvider()
								.jit()
								.build())
				.withCube(
						StartBuilding.cube("OverviewBitmapCube")
								.withContributorsCount()
								.withSingleLevelDimension("Product")
								.withPropertyName("product")
								.withSingleLevelDimension("Date")
								.withPropertyName("date")
								.withDimension("Operations")
								.withHierarchy("Sales")
								.withLevel("Seller")
								.withPropertyName("seller_company")
								.withLevel("Buyer")
								.withPropertyName("buyer_company")
								.withHierarchy("Purchases")
								.withLevel("Buyer")
								.withPropertyName("buyer_company")
								.withLevel("Seller")
								.withPropertyName("seller_company")
								.withEpochDimension()
								.withAggregateProvider()
								.bitmap()
								.build())
				.withCube(
						StartBuilding.cube("OverviewCubeWithPartialProvider")
								.withContributorsCount()
								.withSingleLevelDimension("Product")
								.withPropertyName("product")
								.withSingleLevelDimension("Date")
								.withPropertyName("date")
								.withDimension("Operations")
								.withHierarchy("Sales")
								.withLevel("Seller")
								.withPropertyName("seller_company")
								.withLevel("Buyer")
								.withPropertyName("buyer_company")
								.withHierarchy("Purchases")
								.withLevel("Buyer")
								.withPropertyName("buyer_company")
								.withLevel("Seller")
								.withPropertyName("seller_company")
								.withEpochDimension()
								.withAggregateProvider()
								.bitmap()
								.withPartialProvider()
								.excludingHierarchy("Operations", "Sales")
								.and("Date", "Date")
								.end()
								.build())
				.build();

		return ActivePivotManagerBuilder.postProcess(managerDescription, schemaDescription);
	}

	public static void fillWithGenericData(IDatastore datastore, IActivePivotManager manager) {
		datastore.edit(
				tm -> {
					final int peopleCount = STORE_PEOPLE_COUNT;
					IntStream.range(0, peopleCount)
							.forEach(
									i -> {
										tm.add(
												"People",
												String.valueOf(i),
												"FN" + (i % 4),
												"LN" + i,
												"Corp" + (i + 1 % 3));
									});
					final int productCount = STORE_PRODUCT_COUNT;
					LongStream.range(0, productCount)
							.forEach(
									i -> {
										tm.add("Products", i, "p" + i);
									});

					final Random r = new Random(47605L);
					IntStream.range(operationsBatch.getAndIncrement(), 1000 * operationsBatch.get())
							.forEach(
									i -> {
										final int seller = r.nextInt(peopleCount);
										int buyer;
										do {
											buyer = r.nextInt(peopleCount);
										} while (buyer == seller);
										tm.add(
												"Sales",
												i,
												String.valueOf(seller),
												String.valueOf(buyer),
												LocalDate.now().plusDays(-r.nextInt(7)),
												(long) r.nextInt(productCount));
									});
				});
	}
}
