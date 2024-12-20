/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.activeviam.activepivot.core.impl.internal.utils.ApplicationInTests;
import com.activeviam.activepivot.core.intf.api.cube.IMultiVersionActivePivot;
import com.activeviam.activepivot.server.impl.private_.observability.memory.MemoryAnalysisService;
import com.activeviam.activepivot.server.spring.api.config.IDatastoreSchemaDescriptionConfig;
import com.activeviam.database.api.query.AliasedField;
import com.activeviam.database.api.query.ListQuery;
import com.activeviam.database.datastore.api.IDatastore;
import com.activeviam.database.datastore.api.transaction.DatastoreTransactionException;
import com.activeviam.database.datastore.api.transaction.ITransactionManager;
import com.activeviam.database.datastore.internal.IInternalDatastore;
import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.cfg.impl.RegistryInitializationConfig;
import com.activeviam.mac.entities.ChunkOwner;
import com.activeviam.mac.entities.CubeOwner;
import com.activeviam.mac.entities.StoreOwner;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.mac.statistic.memory.visitor.impl.EpochView;
import com.activeviam.tech.core.api.agent.AgentException;
import com.activeviam.tech.observability.internal.memory.AMemoryStatistic;
import com.activeviam.tech.records.api.ICursor;
import com.activeviam.tech.records.api.IRecordReader;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestViewEpochs extends ATestMemoryStatistic {

  private ApplicationInTests<IDatastore> monitoredApp;
  private ApplicationInTests<IInternalDatastore> monitoringApp;
  private AMemoryStatistic statistics;

  @BeforeAll
  public static void setupRegistry() {
    RegistryInitializationConfig.setupRegistry();
  }

  @BeforeEach
  public void setup() throws AgentException, DatastoreTransactionException {
    initializeApplication();

    final Path exportPath = generateMemoryStatistics();

    this.statistics = loadMemoryStatFromFolder(exportPath);

    initializeMonitoringApplication(this.statistics);

    IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);
    Assertions.assertThat(pivot).isNotNull();
  }

  private void initializeApplication() throws DatastoreTransactionException {
    this.monitoredApp = createMicroApplicationWithIsolatedStoreAndKeepAllEpochPolicy();

    final ITransactionManager transactionManager =
        this.monitoredApp.getDatabase().getTransactionManager();

    // epoch 1 -> store A + cube
    transactionManager.startTransaction("A");
    IntStream.range(0, 10).forEach(i -> transactionManager.add("A", i, 0.));
    transactionManager.commitTransaction();

    // epoch 2 -> store B
    transactionManager.startTransaction("B");
    IntStream.range(0, 10).forEach(i -> transactionManager.add("B", i, 0.));
    transactionManager.commitTransaction();

    // epoch 3 -> store A + cube
    transactionManager.startTransaction("A");
    IntStream.range(10, 20).forEach(i -> transactionManager.add("A", i, 1.));
    transactionManager.commitTransaction();

    // epoch 4 -> store B
    transactionManager.startTransaction("B");
    IntStream.range(10, 20).forEach(i -> transactionManager.add("B", i, 1.));
    transactionManager.commitTransaction();
  }

  private Path generateMemoryStatistics() {
    this.monitoredApp.getDatabase().getEpochManager().forceDiscardEpochs(node -> true);
    performGC();

    final MemoryAnalysisService analysisService =
        (MemoryAnalysisService)
            createService(this.monitoredApp.getDatabase(), this.monitoredApp.getManager());
    return analysisService.exportApplication("testEpochs");
  }

  private void initializeMonitoringApplication(final AMemoryStatistic data) throws AgentException {
    final ManagerDescriptionConfig config = new ManagerDescriptionConfig();
    final IDatastoreSchemaDescriptionConfig schemaConfig =
        new MemoryAnalysisDatastoreDescriptionConfig();

    this.monitoringApp =
        ApplicationInTests.builder()
            .withDatastore(schemaConfig.datastoreSchemaDescription())
            .withManager(config.managerDescription())
            .build();

    ATestMemoryStatistic.feedMonitoringApplication(
        monitoringApp.getDatabase(), List.of(data), "testViewEpochs");
  }

  @AfterEach
  public void tearDown() {
    this.monitoringApp.close();
  }

  @Test
  public void testStatisticConsistency() {
    assertLoadsCorrectly(this.statistics.getChildren(), TestEpochs.class);
  }

  @Test
  public void testExpectedEpochs() {
    final Multimap<ChunkOwner, Long> epochIds = retrieveEpochIdsPerOwner();

    final ChunkOwner storeA = new StoreOwner("A");
    final ChunkOwner storeB = new StoreOwner("B");
    final ChunkOwner cube = new CubeOwner("Cube");

    SoftAssertions.assertSoftly(
        assertions -> {
          assertions
              .assertThat(epochIds.keySet())
              .as("application owners")
              .containsExactlyInAnyOrder(storeA, storeB, cube);

          assertions
              .assertThat(epochIds.get(storeA))
              .as("store A epochs")
              .containsExactlyInAnyOrder(1L, 2L, 3L, 4L);
          assertions
              .assertThat(epochIds.get(storeB))
              .as("store B epochs")
              .containsExactlyInAnyOrder(1L, 2L, 3L, 4L);
          assertions
              .assertThat(epochIds.get(cube))
              .as("cube epochs")
              .containsExactlyInAnyOrder(1L, 3L);
        });
  }

  @Test
  public void testViewEpochMapping() {
    final Map<ChunkOwner, Multimap<Long, Long>> viewEpochs = retrieveViewEpochIdsPerOwner();

    final ChunkOwner storeA = new StoreOwner("A");
    final ChunkOwner storeB = new StoreOwner("B");
    final ChunkOwner cube = new CubeOwner("Cube");

    SoftAssertions.assertSoftly(
        assertions -> {
          assertions
              .assertThat(viewEpochs.keySet())
              .as("application owners")
              .containsExactlyInAnyOrder(storeA, storeB, cube);

          assertions
              .assertThat(viewEpochs.get(storeA).asMap())
              .as("store A epochs")
              .containsExactlyInAnyOrderEntriesOf(
                  Map.of(
                      1L,
                      Collections.singleton(1L),
                      2L,
                      Collections.singleton(2L),
                      3L,
                      Collections.singleton(3L),
                      4L,
                      Collections.singleton(4L)));
          assertions
              .assertThat(viewEpochs.get(storeB).asMap())
              .as("store B epochs")
              .containsExactlyInAnyOrderEntriesOf(
                  Map.of(
                      1L,
                      Collections.singleton(1L),
                      2L,
                      Collections.singleton(2L),
                      3L,
                      Collections.singleton(3L),
                      4L,
                      Collections.singleton(4L)));
          assertions
              .assertThat(viewEpochs.get(cube).asMap())
              .as("cube epochs")
              .containsExactlyInAnyOrderEntriesOf(Map.of(1L, Set.of(1L, 2L), 3L, Set.of(3L, 4L)));
        });
  }

  protected Multimap<ChunkOwner, Long> retrieveEpochIdsPerOwner() {
    ListQuery query =
        this.monitoringApp
            .getDatabase()
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.CHUNK_STORE)
            .withoutCondition()
            .withAliasedFields(
                AliasedField.fromFieldName(DatastoreConstants.OWNER__OWNER),
                AliasedField.fromFieldName(DatastoreConstants.VERSION__EPOCH_ID))
            .toQuery();
    final ICursor cursor =
        this.monitoringApp.getDatabase().getHead("master").getQueryRunner().listQuery(query).run();

    final Multimap<ChunkOwner, Long> epochs = HashMultimap.create();
    for (final IRecordReader record : cursor) {
      epochs.put((ChunkOwner) record.read(0), record.readLong(1));
    }

    cursor.close();
    return epochs;
  }

  protected Map<ChunkOwner, Multimap<Long, Long>> retrieveViewEpochIdsPerOwner() {
    final ListQuery query =
        this.monitoringApp
            .getDatabase()
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.EPOCH_VIEW_STORE)
            .withoutCondition()
            .withAliasedFields(
                AliasedField.fromFieldName(DatastoreConstants.EPOCH_VIEW__OWNER),
                AliasedField.fromFieldName(DatastoreConstants.EPOCH_VIEW__BASE_EPOCH_ID),
                AliasedField.fromFieldName(DatastoreConstants.EPOCH_VIEW__VIEW_EPOCH_ID))
            .toQuery();
    final ICursor cursor =
        this.monitoringApp.getDatabase().getHead("master").getQueryRunner().listQuery(query).run();

    final Map<ChunkOwner, Multimap<Long, Long>> epochs = new HashMap<>();
    for (final IRecordReader record : cursor) {
      epochs
          .computeIfAbsent((ChunkOwner) record.read(0), key -> HashMultimap.create())
          .put(record.readLong(1), ((EpochView) record.read(2)).getEpochId());
    }

    cursor.close();
    return epochs;
  }
}
