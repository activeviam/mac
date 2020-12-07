/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.statistic.memory;

import static com.activeviam.mac.memory.DatastoreConstants.CHUNK_ID;
import static com.activeviam.mac.memory.DatastoreConstants.CHUNK_STORE;
import static com.activeviam.mac.memory.DatastoreConstants.CHUNK__CLASS;
import static com.activeviam.mac.memory.DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE;
import static com.activeviam.mac.memory.DatastoreConstants.CHUNK__OFF_HEAP_SIZE;
import static com.activeviam.mac.memory.DatastoreConstants.CHUNK__PARENT_ID;
import static com.activeviam.mac.memory.DatastoreConstants.OWNER__COMPONENT;
import static com.activeviam.mac.memory.DatastoreConstants.OWNER__OWNER;
import static com.activeviam.mac.memory.DatastoreConstants.VERSION__EPOCH_ID;

import com.activeviam.builders.FactFilterConditions;
import com.activeviam.copper.HierarchyIdentifier;
import com.activeviam.mac.TestMemoryStatisticBuilder;
import com.activeviam.mac.entities.NoOwner;
import com.activeviam.mac.memory.AnalysisDatastoreFeeder;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription.ParentType;
import com.activeviam.pivot.builders.StartBuilding;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.qfs.chunk.direct.impl.SlabDirectChunkAllocator;
import com.qfs.chunk.impl.Chunks;
import com.qfs.condition.ICondition;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.junit.ResourceRule;
import com.qfs.literal.ILiteralType;
import com.qfs.messenger.impl.LocalMessenger;
import com.qfs.monitoring.memory.impl.OnHeapPivotMemoryQuantifierPlugin;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils.StatisticsSummary;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.impl.MemoryStatisticBuilder;
import com.qfs.multiversion.impl.KeepAllEpochPolicy;
import com.qfs.multiversion.impl.KeepLastEpochPolicy;
import com.qfs.pivot.monitoring.impl.MemoryAnalysisService;
import com.qfs.pivot.monitoring.impl.MemoryStatisticSerializerUtil;
import com.qfs.service.monitoring.IMemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.qfs.store.NoTransactionException;
import com.qfs.store.build.impl.UnitTestDatastoreBuilder;
import com.qfs.store.impl.Datastore;
import com.qfs.store.query.IDictionaryCursor;
import com.qfs.store.record.IRecordFormat;
import com.qfs.store.record.IRecordReader;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.qfs.store.transaction.ITransactionManager;
import com.qfs.util.impl.QfsFileTestUtils;
import com.qfs.util.impl.ThrowingLambda;
import com.qfs.util.impl.ThrowingLambda.ThrowingBiConsumer;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.impl.ActivePivotDatastorePostProcessor;
import com.quartetfs.biz.pivot.definitions.impl.ActivePivotManagerDescription;
import com.quartetfs.biz.pivot.impl.ActivePivotManagerBuilder;
import com.quartetfs.biz.pivot.test.util.PivotTestUtils;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.impl.Pair;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import test.util.impl.DatastoreTestUtils;

public abstract class ATestMemoryStatistic {

  public static final int STORE_PEOPLE_COUNT = 10;
  public static final int STORE_PRODUCT_COUNT = 20;

  public static final int SINGLE_VALUE = 10;

  public static final int MICROAPP_CHUNK_SIZE = 256;
  public static final int MICROAPP_VECTOR_BLOCK_SIZE = 64;

  public static final int MAX_GC_STEPS = 10;

  protected static final String VECTOR_STORE_NAME = "vectorStore";

  public static AtomicInteger operationsBatch = new AtomicInteger();

  @ClassRule public static final ResourceRule resources = new ResourceRule();

  @BeforeClass
  public static void setUpRegistry() {
    PivotTestUtils.setUpRegistry(OnHeapPivotMemoryQuantifierPlugin.class);
  }

  public static void performGC() {
    // Make sure that no thread holds stale blocks.
    DatastoreTestUtils.resetAllThreadsVectorAllocator();

    /*
     * Note. We can't rely on calling MemUtils.runGC()
     * because on some servers (alto), it seems not enough.
     * Plus, MemUtils relies on on heap memory....
     */
    final SlabDirectChunkAllocator allocator = (SlabDirectChunkAllocator) Chunks.allocator();
    for (int i = 0; i < MAX_GC_STEPS; i++) {
      try {
        System.gc();

        Thread.sleep(1 << i); // give gc some times.

        // create a soft assertion that allows getting the assertions results of all assertions
        // even if the first assertion is already false.
        break;
      } catch (Throwable e) {
        if (i == MAX_GC_STEPS - 1) {
          // MAX_GC was not enough, throw !
          throw new RuntimeException(
              "Incorrect direct memory count or reserved memory after " + MAX_GC_STEPS + " gcs.",
              e);
        }
      }
    }
  }

  static void createApplication(
      final ThrowingLambda.ThrowingBiConsumer<Datastore, IActivePivotManager> actions) {
    final IDatastoreSchemaDescription datastoreSchema =
        StartBuilding.datastoreSchema()
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
    final IActivePivotManagerDescription userManagerDescription =
        StartBuilding.managerDescription()
            .withSchema()
            .withSelection(
                StartBuilding.selection(datastoreSchema)
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
    final IActivePivotManagerDescription managerDescription =
        ActivePivotManagerBuilder.postProcess(userManagerDescription, datastoreSchema);

    final Datastore datastore =
        (Datastore)
            resources.create(
                () ->
                    StartBuilding.datastore()
                        .setSchemaDescription(datastoreSchema)
                        .addSchemaDescriptionPostProcessors(
                            ActivePivotDatastorePostProcessor.createFrom(managerDescription))
                        .build());
    final IActivePivotManager manager;
    try {
      manager =
          StartBuilding.manager()
              .setDescription(managerDescription)
              .setDatastoreAndPermissions(datastore)
              .buildAndStart();
    } catch (AgentException e) {
      throw new RuntimeException("Cannot create manager", e);
    }
    resources.register(manager::stop);

    actions.accept(datastore, manager);
  }

  static void createMinimalApplication(
      final ThrowingLambda.ThrowingBiConsumer<Datastore, IActivePivotManager> actions) {

    final IDatastoreSchemaDescription datastoreSchema =
        StartBuilding.datastoreSchema()
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
    final IActivePivotManagerDescription userManagerDescription =
        StartBuilding.managerDescription()
            .withSchema()
            .withSelection(
                StartBuilding.selection(datastoreSchema)
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
    final IActivePivotManagerDescription managerDescription =
        ActivePivotManagerBuilder.postProcess(userManagerDescription, datastoreSchema);

    final Datastore datastore =
        (Datastore)
            resources.create(
                () ->
                    StartBuilding.datastore()
                        .setSchemaDescription(datastoreSchema)
                        .setEpochManagementPolicy(new KeepAllEpochPolicy())
                        .addSchemaDescriptionPostProcessors(
                            ActivePivotDatastorePostProcessor.createFrom(managerDescription))
                        .build());
    final IActivePivotManager manager;
    try {
      manager =
          StartBuilding.manager()
              .setDescription(managerDescription)
              .setDatastoreAndPermissions(datastore)
              .buildAndStart();
    } catch (AgentException e) {
      throw new RuntimeException("Cannot create manager", e);
    }
    resources.register(manager::stop);

    actions.accept(datastore, manager);
  }

  /**
   * Fills the datastore created by {@link #createMinimalApplication(ThrowingBiConsumer)}.
   *
   * @param datastore datastore to fill
   */
  static void fillApplicationMinimal(final Datastore datastore) {
    datastore.edit(
        tm -> {
          final int peopleCount = STORE_PEOPLE_COUNT;
          final int productCount = STORE_PRODUCT_COUNT;

          final Random r = new Random(47605);
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

  /**
   * Fills the datastore created by {@link #createApplication(ThrowingLambda.ThrowingBiConsumer)}.
   *
   * @param datastore datastore to fill
   */
  static void fillApplication(final Datastore datastore) {
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

          final Random r = new Random(47605);
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

  /**
   * Fills the datastore created by {@link #createApplication(ThrowingLambda.ThrowingBiConsumer)}.
   *
   * @param datastore datastore to fill
   */
  static void fillApplicationMinimalWithSingleValue(final Datastore datastore) {
    datastore.edit(
        tm -> {
          final int peopleCount = STORE_PEOPLE_COUNT;

          final Random r = new Random(47605);
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
                        (long) SINGLE_VALUE);
                  });
        });
  }

  /**
   * Fills the datastore created by {@link #createApplication(ThrowingLambda.ThrowingBiConsumer)}.
   *
   * @param datastore datastore to fill
   */
  static void editApplicationMinimalWithSingleValue(final Datastore datastore) {
    datastore.edit(
        tm -> {
          final int peopleCount = STORE_PEOPLE_COUNT;

          final Random r = new Random(47605);
          IntStream.range(0, 1000)
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
                        (long) SINGLE_VALUE);
                  });
        });
  }

  /**
   * Fills the datastore created by {@link #createApplication(ThrowingLambda.ThrowingBiConsumer)}
   * and add some data on another branch tha "master"
   *
   * @param datastore datastore to fill
   * @throws DatastoreTransactionException
   * @throws IllegalArgumentException
   */
  static void fillApplicationWithBranches(
      final Datastore datastore, Collection<String> branches, boolean minimalFilling)
      throws IllegalArgumentException, DatastoreTransactionException {
    if (minimalFilling) {
      fillApplicationMinimal(datastore);
    } else {
      fillApplication(datastore);
    }
    branches.forEach(
        br_string -> {
          ITransactionManager tm = datastore.getTransactionManager();
          try {
            tm.startTransactionOnBranch(br_string, "Sales");
          } catch (IllegalArgumentException | DatastoreTransactionException e) {
            throw new RuntimeException(e);
          }
          final Random r = new Random(47605);
          IntStream.range(operationsBatch.getAndIncrement(), 1000 * operationsBatch.get())
              .forEach(
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
          try {
            tm.commitTransaction();
          } catch (NoTransactionException | DatastoreTransactionException e) {
            throw new RuntimeException(e);
          }
        });
  }

  static IMemoryAnalysisService createService(
      final IDatastore datastore, final IActivePivotManager manager) {
    final Path dumpDirectory =
        QfsFileTestUtils.createTempDirectory(TestMemoryStatisticLoading.class);
    return new MemoryAnalysisService(
        datastore, manager, datastore.getEpochManager(), dumpDirectory);
  }

  static IDatastore createAnalysisDatastore() {
    final IDatastoreSchemaDescription desc = new MemoryAnalysisDatastoreDescription();
    IDatastore d =
        resources.create(
            () -> {
              return StartBuilding.datastore().setSchemaDescription(desc).build();
            });

    return d;
  }

  /**
   * Creates a minimal application with a single store {@link #VECTOR_STORE_NAME} with the following
   * fields: - vectorId - int - vectorInt1 - int[] (block of 35 entries) - vectorInt2 - int[]
   * (blocks of 20 entries) - vectorLong - long[] (blocks of 30 entries) The store has chunks of 10
   * rows.
   */
  static void createApplicationWithVector(
      final boolean useVectorsAsMeasures,
      final ThrowingLambda.ThrowingBiConsumer<IDatastore, IActivePivotManager> actions) {
    final IDatastoreSchemaDescription schemaDescription =
        StartBuilding.datastoreSchema()
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
    final IActivePivotManagerDescription userManagerDescription =
        StartBuilding.managerDescription()
            .withSchema()
            .withSelection(
                StartBuilding.selection(schemaDescription)
                    .fromBaseStore(VECTOR_STORE_NAME)
                    .withAllFields()
                    .build())
            .withCube(
                StartBuilding.cube("C")
                    .withMeasures(
                        builder -> {
                          if (useVectorsAsMeasures) {
                            return builder
                                .withAggregatedMeasure()
                                .sum("vectorInt1")
                                .withAggregatedMeasure()
                                .avg("vectorLong");
                          } else {
                            return builder.withContributorsCount();
                          }
                        })
                    .withSingleLevelDimension("Id")
                    .withPropertyName("vectorId")
                    .withAggregateProvider()
                    .leaf()
                    .build())
            .build();
    final IActivePivotManagerDescription managerDescription =
        ActivePivotManagerBuilder.postProcess(userManagerDescription, schemaDescription);

    final IDatastore datastore =
        resources.create(
            () ->
                new UnitTestDatastoreBuilder()
                    .setSchemaDescription(schemaDescription)
                    .addSchemaDescriptionPostProcessors(
                        ActivePivotDatastorePostProcessor.createFrom(managerDescription))
                    .build());
    final IActivePivotManager manager;
    try {
      manager =
          StartBuilding.manager()
              .setDescription(managerDescription)
              .setDatastoreAndPermissions(datastore)
              .buildAndStart();
    } catch (AgentException e) {
      throw new RuntimeException("Cannot start the manager", e);
    }

    actions.accept(datastore, manager);
  }

  static void commitDataInDatastoreWithVectors(
      final IDatastore monitoredDatastore, final boolean commitDuplicatedVectors)
      throws DatastoreTransactionException {
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
    monitoredDatastore.edit(
        tm -> {
          for (int i = 0; i < nbOfVectors; i++) {
            tm.add(VECTOR_STORE_NAME, i, v1, v3, v2);
          }
        });

    // If commitDuplicatedVectors, take already registered vector and re-commit it in a different
    // field
    if (commitDuplicatedVectors) {
      IDictionaryCursor cursor =
          monitoredDatastore
              .getHead()
              .getQueryManager()
              .forStore(VECTOR_STORE_NAME)
              .withoutCondition()
              .selecting("vectorInt1")
              .run();

      final Object vec = cursor.next() ? cursor.getRecord().read("vectorInt1") : null;

      monitoredDatastore.edit(
          tm -> {
            tm.add(VECTOR_STORE_NAME, 0, v1, vec, v2);
          });
    }
  }

  /**
   * Builds a minimal application with one field <i>id</i> in the store <i>A</i>, loaded into a cube
   * <i>Cube</i> with a single hierarchy <i>id</i>.
   */
  static Pair<IDatastore, IActivePivotManager> createMicroApplication() {

    final IDatastoreSchemaDescription schemaDescription =
        StartBuilding.datastoreSchema()
            .withStore(
                StartBuilding.store()
                    .withStoreName("A")
                    .withField("id", ILiteralType.INT)
                    .asKeyField()
                    .withChunkSize(MICROAPP_CHUNK_SIZE)
                    .build())
            .build();

    final IActivePivotManagerDescription userManagerDescription =
        StartBuilding.managerDescription()
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

    final IActivePivotManagerDescription managerDescription =
        ActivePivotManagerBuilder.postProcess(userManagerDescription, schemaDescription);
    IDatastore datastore =
        resources.create(
            () ->
                new UnitTestDatastoreBuilder()
                    .setSchemaDescription(schemaDescription)
                    .addSchemaDescriptionPostProcessors(
                        ActivePivotDatastorePostProcessor.createFrom(managerDescription))
                    .setEpochManagementPolicy(new KeepLastEpochPolicy())
                    .build());
    final IActivePivotManager manager;
    try {
      manager =
          StartBuilding.manager()
              .setDescription(managerDescription)
              .setDatastoreAndPermissions(datastore)
              .buildAndStart();
    } catch (AgentException e) {
      throw new RuntimeException("Cannot start manager", e);
    }
    return new Pair<>(datastore, manager);
  }

  static Pair<IDatastore, IActivePivotManager> createMicroApplicationWithIndexedFields() {
    final IDatastoreSchemaDescription schemaDescription =
        StartBuilding.datastoreSchema()
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
                    .withChunkSize(MICROAPP_CHUNK_SIZE)
                    .build())
            .withStore(
                StartBuilding.store()
                    .withStoreName("B")
                    .withField("id0", ILiteralType.INT)
                    .asKeyField()
                    .build())
            .withReference(
                StartBuilding.reference()
                    .fromStore("A")
                    .toStore("B")
                    .withName("ref")
                    .withMapping("id0", "id0")
                    .build())
            .build();

    final IActivePivotManagerDescription userManagerDescription =
        new ActivePivotManagerDescription();

    final IActivePivotManagerDescription managerDescription =
        ActivePivotManagerBuilder.postProcess(userManagerDescription, schemaDescription);
    IDatastore datastore =
        resources.create(
            () ->
                new UnitTestDatastoreBuilder()
                    .setSchemaDescription(schemaDescription)
                    .addSchemaDescriptionPostProcessors(
                        ActivePivotDatastorePostProcessor.createFrom(managerDescription))
                    .setEpochManagementPolicy(new KeepLastEpochPolicy())
                    .build());
    final IActivePivotManager manager;
    try {
      manager =
          StartBuilding.manager()
              .setDescription(managerDescription)
              .setDatastoreAndPermissions(datastore)
              .buildAndStart();
    } catch (AgentException e) {
      throw new RuntimeException("Cannot start manager", e);
    }
    return new Pair<>(datastore, manager);
  }

  static Pair<IDatastore, IActivePivotManager> createMicroApplicationWithKeepAllEpochPolicy() {

    final IDatastoreSchemaDescription schemaDescription =
        StartBuilding.datastoreSchema()
            .withStore(
                StartBuilding.store()
                    .withStoreName("A")
                    .withField("id", ILiteralType.INT)
                    .asKeyField()
                    .withField("value", ILiteralType.DOUBLE)
                    .asKeyField()
                    .withChunkSize(MICROAPP_CHUNK_SIZE)
                    .withValuePartitioningOn("value")
                    .build())
            .build();

    final IActivePivotManagerDescription userManagerDescription =
        StartBuilding.managerDescription()
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

    final IActivePivotManagerDescription managerDescription =
        ActivePivotManagerBuilder.postProcess(userManagerDescription, schemaDescription);
    IDatastore datastore =
        resources.create(
            () ->
                new UnitTestDatastoreBuilder()
                    .setSchemaDescription(schemaDescription)
                    .addSchemaDescriptionPostProcessors(
                        ActivePivotDatastorePostProcessor.createFrom(managerDescription))
                    .setEpochManagementPolicy(new KeepAllEpochPolicy())
                    .build());
    final IActivePivotManager manager;
    try {
      manager =
          StartBuilding.manager()
              .setDescription(managerDescription)
              .setDatastoreAndPermissions(datastore)
              .buildAndStart();
    } catch (AgentException e) {
      throw new RuntimeException("Cannot start manager", e);
    }
    return new Pair<>(datastore, manager);
  }

  static Pair<IDatastore, IActivePivotManager>
      createMicroApplicationWithIsolatedStoreAndKeepAllEpochPolicy() {
    final IDatastoreSchemaDescription schemaDescription =
        StartBuilding.datastoreSchema()
            .withStore(
                StartBuilding.store()
                    .withStoreName("A")
                    .withField("id", ILiteralType.INT)
                    .asKeyField()
                    .withField("value", ILiteralType.DOUBLE)
                    .asKeyField()
                    .withChunkSize(MICROAPP_CHUNK_SIZE)
                    .withValuePartitioningOn("value")
                    .build())
            .withStore(
                StartBuilding.store()
                    .withStoreName("B")
                    .withField("id", ILiteralType.INT)
                    .asKeyField()
                    .withField("value", ILiteralType.DOUBLE)
                    .asKeyField()
                    .withChunkSize(MICROAPP_CHUNK_SIZE)
                    .withValuePartitioningOn("value")
                    .build())
            .build();

    final IActivePivotManagerDescription userManagerDescription =
        StartBuilding.managerDescription()
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

    final IActivePivotManagerDescription managerDescription =
        ActivePivotManagerBuilder.postProcess(userManagerDescription, schemaDescription);
    IDatastore datastore =
        resources.create(
            () ->
                new UnitTestDatastoreBuilder()
                    .setSchemaDescription(schemaDescription)
                    .addSchemaDescriptionPostProcessors(
                        ActivePivotDatastorePostProcessor.createFrom(managerDescription))
                    .setEpochManagementPolicy(new KeepAllEpochPolicy())
                    .build());
    final IActivePivotManager manager;
    try {
      manager =
          StartBuilding.manager()
              .setDescription(managerDescription)
              .setDatastoreAndPermissions(datastore)
              .buildAndStart();
    } catch (AgentException e) {
      throw new RuntimeException("Cannot start manager", e);
    }
    return new Pair<>(datastore, manager);
  }

  static Pair<IDatastore, IActivePivotManager>
      createDistributedApplicationWithKeepAllEpochPolicy() {

    final IDatastoreSchemaDescription schemaDescription =
        StartBuilding.datastoreSchema()
            .withStore(
                StartBuilding.store()
                    .withStoreName("A")
                    .withField("id", ILiteralType.INT)
                    .asKeyField()
                    .withField("value", ILiteralType.DOUBLE)
                    .asKeyField()
                    .withChunkSize(MICROAPP_CHUNK_SIZE)
                    .withValuePartitioningOn("value")
                    .build())
            .build();

    final IActivePivotManagerDescription userManagerDescription =
        StartBuilding.managerDescription()
            .withSchema()
            .withSelection(
                StartBuilding.selection(schemaDescription)
                    .fromBaseStore("A")
                    .withAllFields()
                    .build())
            .withCube(
                StartBuilding.cube()
                    .withName("Data")
                    .withAggregatedMeasure()
                    .sum("value")
                    .withSingleLevelDimension("id")
                    .build())
            .withDistributedCube(
                StartBuilding.cube("QueryCubeA")
                    .withContributorsCount()
                    .asQueryCube()
                    .withClusterDefinition()
                    .withClusterId("cluster")
                    .withMessengerDefinition()
                    .withKey(LocalMessenger.PLUGIN_KEY)
                    .withNoProperty()
                    .end()
                    .withApplication("app")
                    .withDistributingFields("value")
                    .end()
                    .withEpochDimension()
                    .build())
            .withDistributedCube(
                StartBuilding.cube("QueryCubeB")
                    .asQueryCube()
                    .withClusterDefinition()
                    .withClusterId("cluster")
                    .withMessengerDefinition()
                    .withKey(LocalMessenger.PLUGIN_KEY)
                    .withNoProperty()
                    .end()
                    .withApplication("app")
                    .withoutDistributingFields()
                    .end()
                    .withEpochDimension()
                    .build())
            .build();

    final IActivePivotManagerDescription managerDescription =
        ActivePivotManagerBuilder.postProcess(userManagerDescription, schemaDescription);
    IDatastore datastore =
        resources.create(
            () ->
                new UnitTestDatastoreBuilder()
                    .setSchemaDescription(schemaDescription)
                    .addSchemaDescriptionPostProcessors(
                        ActivePivotDatastorePostProcessor.createFrom(managerDescription))
                    .setEpochManagementPolicy(new KeepAllEpochPolicy())
                    .build());
    final IActivePivotManager manager;
    try {
      manager =
          StartBuilding.manager()
              .setDescription(managerDescription)
              .setDatastoreAndPermissions(datastore)
              .buildAndStart();
    } catch (AgentException e) {
      throw new RuntimeException("Cannot start manager", e);
    }
    return new Pair<>(datastore, manager);
  }

  static Pair<IDatastore, IActivePivotManager> createMicroApplicationWithSharedVectorField() {

    final IDatastoreSchemaDescription schemaDescription =
        StartBuilding.datastoreSchema()
            .withStore(
                StartBuilding.store()
                    .withStoreName("A")
                    .withField("id", ILiteralType.INT)
                    .asKeyField()
                    .withVectorField("vector1", ILiteralType.DOUBLE)
                    .withVectorBlockSize(MICROAPP_VECTOR_BLOCK_SIZE)
                    .withVectorField("vector2", ILiteralType.DOUBLE)
                    .withVectorBlockSize(MICROAPP_VECTOR_BLOCK_SIZE)
                    .withChunkSize(MICROAPP_CHUNK_SIZE)
                    .build())
            .build();

    final IActivePivotManagerDescription userManagerDescription =
        new ActivePivotManagerDescription();

    final IActivePivotManagerDescription managerDescription =
        ActivePivotManagerBuilder.postProcess(userManagerDescription, schemaDescription);
    IDatastore datastore =
        resources.create(
            () ->
                new UnitTestDatastoreBuilder()
                    .setSchemaDescription(schemaDescription)
                    .addSchemaDescriptionPostProcessors(
                        ActivePivotDatastorePostProcessor.createFrom(managerDescription))
                    .setEpochManagementPolicy(new KeepLastEpochPolicy())
                    .build());
    final IActivePivotManager manager;
    try {
      manager =
          StartBuilding.manager()
              .setDescription(managerDescription)
              .setDatastoreAndPermissions(datastore)
              .buildAndStart();
    } catch (AgentException e) {
      throw new RuntimeException("Cannot start manager", e);
    }
    return new Pair<>(datastore, manager);
  }

  static Pair<IDatastore, IActivePivotManager> createMicroApplicationWithReference()
      throws AgentException {

    final IDatastoreSchemaDescription schemaDescription =
        StartBuilding.datastoreSchema()
            .withStore(
                StartBuilding.store()
                    .withStoreName("A")
                    .withField("id", ILiteralType.INT)
                    .asKeyField()
                    .withField("val", ILiteralType.INT)
                    .withChunkSize(MICROAPP_CHUNK_SIZE)
                    .build())
            .withStore(
                StartBuilding.store()
                    .withStoreName("B")
                    .withField("tgt_id", ILiteralType.INT)
                    .asKeyField()
                    .withChunkSize(MICROAPP_CHUNK_SIZE)
                    .build())
            .withReference(
                StartBuilding.reference()
                    .fromStore("A")
                    .toStore("B")
                    .withName("AToB")
                    .withMapping("val", "tgt_id")
                    .build())
            .build();

    final IActivePivotManagerDescription userManagerDescription =
        StartBuilding.managerDescription()
            .withSchema()
            .withSelection(
                StartBuilding.selection(schemaDescription)
                    .fromBaseStore("A")
                    .withAllReachableFields()
                    .build())
            .withCube(
                StartBuilding.cube("Cube")
                    .withContributorsCount()
                    .withSingleLevelDimension("id")
                    .asDefaultHierarchy()
                    .build())
            .build();
    final IActivePivotManagerDescription managerDescription =
        ActivePivotManagerBuilder.postProcess(userManagerDescription, schemaDescription);

    IDatastore datastore =
        (Datastore)
            resources.create(
                () ->
                    new UnitTestDatastoreBuilder()
                        .setSchemaDescription(schemaDescription)
                        .addSchemaDescriptionPostProcessors(
                            ActivePivotDatastorePostProcessor.createFrom(managerDescription))
                        .setEpochManagementPolicy(new KeepLastEpochPolicy())
                        .build());
    return new Pair<IDatastore, IActivePivotManager>(
        datastore,
        StartBuilding.manager()
            .setDescription(managerDescription)
            .setDatastoreAndPermissions(datastore)
            .buildAndStart());
  }

  static Pair<IDatastore, IActivePivotManager> createMicroApplicationWithReferenceAndSameFieldName()
      throws AgentException {

    final IDatastoreSchemaDescription schemaDescription =
        StartBuilding.datastoreSchema()
            .withStore(
                StartBuilding.store()
                    .withStoreName("A")
                    .withField("id", ILiteralType.INT)
                    .asKeyField()
                    .withField("val", ILiteralType.INT)
                    .withChunkSize(MICROAPP_CHUNK_SIZE)
                    .build())
            .withStore(
                StartBuilding.store()
                    .withStoreName("B")
                    .withField("tgt_id", ILiteralType.INT)
                    .asKeyField()
                    .withField("val", ILiteralType.INT)
                    .withChunkSize(MICROAPP_CHUNK_SIZE)
                    .build())
            .withReference(
                StartBuilding.reference()
                    .fromStore("A")
                    .toStore("B")
                    .withName("AToB")
                    .withMapping("val", "tgt_id")
                    .build())
            .build();

    final IActivePivotManagerDescription userManagerDescription =
        StartBuilding.managerDescription()
            .withSchema()
            .withSelection(
                StartBuilding.selection(schemaDescription)
                    .fromBaseStore("A")
                    .withAllReachableFields()
                    .build())
            .withCube(
                StartBuilding.cube("Cube")
                    .withContributorsCount()
                    .withSingleLevelDimension("id")
                    .asDefaultHierarchy()
                    .build())
            .build();
    final IActivePivotManagerDescription managerDescription =
        ActivePivotManagerBuilder.postProcess(userManagerDescription, schemaDescription);

    IDatastore datastore =
        (Datastore)
            resources.create(
                () ->
                    new UnitTestDatastoreBuilder()
                        .setSchemaDescription(schemaDescription)
                        .addSchemaDescriptionPostProcessors(
                            ActivePivotDatastorePostProcessor.createFrom(managerDescription))
                        .setEpochManagementPolicy(new KeepLastEpochPolicy())
                        .build());
    return new Pair<IDatastore, IActivePivotManager>(
        datastore,
        StartBuilding.manager()
            .setDescription(managerDescription)
            .setDatastoreAndPermissions(datastore)
            .buildAndStart());
  }

  static Pair<IDatastore, IActivePivotManager> createMicroApplicationWithLeafBitmap()
      throws AgentException {

    final IDatastoreSchemaDescription schemaDescription =
        StartBuilding.datastoreSchema()
            .withStore(
                StartBuilding.store()
                    .withStoreName("A")
                    .withField("id", ILiteralType.INT)
                    .asKeyField()
                    .withChunkSize(MICROAPP_CHUNK_SIZE)
                    .build())
            .build();
    final IActivePivotManagerDescription userManagerDescription =
        StartBuilding.managerDescription()
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
                    .withAggregateProvider()
                    .leaf()
                    .build())
            .build();
    final IActivePivotManagerDescription managerDescription =
        ActivePivotManagerBuilder.postProcess(userManagerDescription, schemaDescription);
    IDatastore datastore =
        (Datastore)
            resources.create(
                () ->
                    new UnitTestDatastoreBuilder()
                        .setSchemaDescription(schemaDescription)
                        .addSchemaDescriptionPostProcessors(
                            ActivePivotDatastorePostProcessor.createFrom(managerDescription))
                        .setEpochManagementPolicy(new KeepLastEpochPolicy())
                        .build());
    return new Pair<IDatastore, IActivePivotManager>(
        datastore,
        StartBuilding.manager()
            .setDescription(managerDescription)
            .setDatastoreAndPermissions(datastore)
            .buildAndStart());
  }

  static Pair<IDatastore, IActivePivotManager> createMicroApplicationWithPartialProviders()
      throws AgentException {

    final IDatastoreSchemaDescription schemaDescription =
        StartBuilding.datastoreSchema()
            .withStore(
                StartBuilding.store()
                    .withStoreName("A")
                    .withField("id", ILiteralType.INT)
                    .asKeyField()
                    .withField("hierId", ILiteralType.INT)
                    .withField("measure1", ILiteralType.DOUBLE)
                    .withField("measure2", ILiteralType.DOUBLE)
                    .withChunkSize(MICROAPP_CHUNK_SIZE)
                    .build())
            .build();
    final IActivePivotManagerDescription userManagerDescription =
        StartBuilding.managerDescription()
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
    final IActivePivotManagerDescription managerDescription =
        ActivePivotManagerBuilder.postProcess(userManagerDescription, schemaDescription);
    IDatastore datastore =
        (Datastore)
            resources.create(
                () ->
                    new UnitTestDatastoreBuilder()
                        .setSchemaDescription(schemaDescription)
                        .addSchemaDescriptionPostProcessors(
                            ActivePivotDatastorePostProcessor.createFrom(managerDescription))
                        .setEpochManagementPolicy(new KeepLastEpochPolicy())
                        .build());
    return new Pair<IDatastore, IActivePivotManager>(
        datastore,
        StartBuilding.manager()
            .setDescription(managerDescription)
            .setDatastoreAndPermissions(datastore)
            .buildAndStart());
  }

  static IDatastore assertLoadsCorrectly(
      final Collection<? extends IMemoryStatistic> statistics, Class<?> klass) {
    final IDatastore monitoringDatastore = createAnalysisDatastore();

    final AnalysisDatastoreFeeder feeder = new AnalysisDatastoreFeeder(statistics, "storeA");
    monitoringDatastore.edit(feeder::feedDatastore);

    final StatisticsSummary statisticsSummary = computeStatisticsSummary(statistics, klass);

    assertDatastoreConsistentWithSummary(monitoringDatastore, statisticsSummary);

    checkForUnclassifiedChunks(monitoringDatastore);
    checkForUnrootedChunks(monitoringDatastore);

    return monitoringDatastore;
  }

  protected static StatisticsSummary computeStatisticsSummary(
      final Collection<? extends IMemoryStatistic> statistics, final Class<?> creatorClass) {
    return MemoryStatisticsTestUtils.getStatisticsSummary(
        new TestMemoryStatisticBuilder()
            .withCreatorClasses(creatorClass)
            .withChildren(statistics)
            .build());
  }

  static void checkForUnclassifiedChunks(IDatastore monitoringDatastore) {
    // Check that all chunks have a parent type/id
    final IDictionaryCursor cursor =
        monitoringDatastore
            .getHead()
            .getQueryRunner()
            .forStore(CHUNK_STORE)
            .withCondition(
                BaseConditions.Or(
                    BaseConditions.Equal(
                        CHUNK__CLOSEST_PARENT_TYPE, IRecordFormat.GLOBAL_DEFAULT_STRING),
                    BaseConditions.Equal(CHUNK__PARENT_ID, IRecordFormat.GLOBAL_DEFAULT_STRING)))
            .selecting(CHUNK_ID, CHUNK__CLASS, CHUNK__CLOSEST_PARENT_TYPE, CHUNK__PARENT_ID)
            .onCurrentThread()
            .run();
    if (cursor.hasNext()) {
      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count += 1;
        System.out.println("Error for " + cursor.getRawRecord());
      }
      throw new AssertionError(count + " chunks without parent type/id");
    }
  }

  static void checkForUnrootedChunks(final IDatastore monitoringDatastore) {
    // Check that all chunks have an owner

    final Set<Long> chunkStoreChunks =
        retrieveAllChunkIds(monitoringDatastore, CHUNK_STORE, CHUNK_ID, BaseConditions.TRUE);
    final Set<Long> ownerStoreChunks =
        retrieveAllChunkIds(
            monitoringDatastore,
            CHUNK_STORE,
            CHUNK_ID,
            BaseConditions.Not(BaseConditions.Equal(OWNER__OWNER, NoOwner.getInstance())));
    final Set<Long> componentStoreChunks =
        retrieveAllChunkIds(
            monitoringDatastore,
            CHUNK_STORE,
            CHUNK_ID,
            BaseConditions.Not(BaseConditions.Equal(OWNER__COMPONENT, ParentType.NO_COMPONENT)));

    Assertions.assertThat(ownerStoreChunks).containsExactlyInAnyOrderElementsOf(chunkStoreChunks);
    Assertions.assertThat(componentStoreChunks)
        .containsExactlyInAnyOrderElementsOf(chunkStoreChunks);
  }

  static Set<Long> retrieveAllChunkIds(
      final IDatastore monitoringDatastore,
      final String storeName,
      final String chunkIdFieldName,
      final ICondition condition) {
    final IDictionaryCursor cursor =
        monitoringDatastore
            .getHead()
            .getQueryRunner()
            .forStore(storeName)
            .withCondition(condition)
            .selecting(chunkIdFieldName)
            .onCurrentThread()
            .run();

    return StreamSupport.stream(cursor.spliterator(), false)
        .map(reader -> reader.readLong(0))
        .collect(Collectors.toSet());
  }

  protected static class VersionedChunkInfo {

    public long epochId;
    public long offHeapSize;
    public String chunkClass;

    public VersionedChunkInfo(long epochId, long offHeapSize, String chunkClass) {
      this.epochId = epochId;
      this.offHeapSize = offHeapSize;
      this.chunkClass = chunkClass;
    }
  }

  /**
   * Asserts the monitoring datastore contains chunks consistent with what the statistics summary
   * says.
   *
   * @param monitoringDatastore The monitoring datastore in which the statistics was loaded.
   * @param statisticsSummary The statistics summary we want to compare the datastore with.
   */
  static void assertDatastoreConsistentWithSummary(
      IDatastore monitoringDatastore, StatisticsSummary statisticsSummary) {

    final Map<Long, VersionedChunkInfo> latestChunkInfos =
        extractLatestChunkInfos(monitoringDatastore);

    final long chunkCount = latestChunkInfos.size();
    final long totalChunkOffHeapSize =
        latestChunkInfos.values().stream().mapToLong(chunk -> chunk.offHeapSize).sum();
    Multimap<String, Long> chunkIdsByClass = HashMultimap.create();
    latestChunkInfos.forEach((key, value) -> chunkIdsByClass.put(value.chunkClass, key));

    SoftAssertions.assertSoftly(
        assertions -> {
          assertions
              .assertThat(totalChunkOffHeapSize)
              .as("off-heap memory computed on monitoring datastore")
              .isEqualTo(statisticsSummary.offHeapMemory);
          assertions
              .assertThat(chunkCount)
              .as("total number of chunks loaded in monitoring store")
              .isEqualTo(statisticsSummary.numberDistinctChunks);
          assertions
              .assertThat(chunkIdsByClass.asMap())
              .as("Classes of the loaded chunks")
              .containsAllEntriesOf(statisticsSummary.chunkIdsByClass);
        });
  }

  static Map<Long, VersionedChunkInfo> extractLatestChunkInfos(final IDatastore datastore) {
    IDictionaryCursor cursor =
        datastore
            .getHead()
            .getQueryRunner()
            .forStore(CHUNK_STORE)
            .withoutCondition()
            .selecting(CHUNK_ID, VERSION__EPOCH_ID, CHUNK__OFF_HEAP_SIZE, CHUNK__CLASS)
            .onCurrentThread()
            .run();

    final Map<Long, VersionedChunkInfo> latestChunkInfos = new HashMap<>();
    for (final IRecordReader reader : cursor) {
      final long chunkId = reader.readLong(0);
      final long epochId = reader.readLong(1);

      final VersionedChunkInfo chunkInfo = latestChunkInfos.get(chunkId);
      if (chunkInfo == null || chunkInfo.epochId < epochId) {
        final long offHeapSize = reader.readLong(2);
        final String chunkClass = (String) reader.read(3);
        latestChunkInfos.put(chunkId, new VersionedChunkInfo(epochId, offHeapSize, chunkClass));
      }
    }

    return latestChunkInfos;
  }

  static IMemoryStatistic loadMemoryStatFromFolder(final Path folderPath) {
    return loadMemoryStatFromFolder(folderPath, __ -> true);
  }

  @SuppressWarnings("unchecked")
  static Collection<IMemoryStatistic> loadDatastoreMemoryStatFromFolder(final Path folderPath) {
    final IMemoryStatistic allStat =
        loadMemoryStatFromFolder(
            folderPath,
            path ->
                path.getFileName().toString().startsWith(MemoryAnalysisService.STORE_FILE_PREFIX));
    return (Collection<IMemoryStatistic>) allStat.getChildren();
  }

  @SuppressWarnings("unchecked")
  static Collection<IMemoryStatistic> loadPivotMemoryStatFromFolder(final Path folderPath) {
    final IMemoryStatistic allStat =
        loadMemoryStatFromFolder(
            folderPath,
            path ->
                path.getFileName().toString().startsWith(MemoryAnalysisService.PIVOT_FILE_PREFIX));
    return (Collection<IMemoryStatistic>) allStat.getChildren();
  }

  static IMemoryStatistic loadMemoryStatFromFolder(
      final Path folderPath, final Predicate<Path> filter) {
    final Stream<Path> fileList;
    try {
      fileList = Files.list(folderPath);
    } catch (IOException e) {
      throw new IllegalArgumentException("Cannot list files under " + folderPath, e);
    }
    final List<IMemoryStatistic> childStats =
        fileList
            .filter(filter)
            .map(
                file -> {
                  try {
                    return MemoryStatisticSerializerUtil.readStatisticFile(file.toFile());
                  } catch (IOException e) {
                    throw new RuntimeException("Cannot read " + file, e);
                  }
                })
            .collect(Collectors.toList());

    return new MemoryStatisticBuilder()
        .withCreatorClasses(TestMemoryStatisticLoading.class)
        .withChildren(childStats)
        .build();
  }
}
