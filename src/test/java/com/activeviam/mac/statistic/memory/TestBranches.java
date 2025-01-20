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
import com.activeviam.database.api.conditions.BaseConditions;
import com.activeviam.database.api.query.AliasedField;
import com.activeviam.database.api.query.ListQuery;
import com.activeviam.database.api.schema.FieldPath;
import com.activeviam.database.datastore.api.IDatastore;
import com.activeviam.database.datastore.api.transaction.DatastoreTransactionException;
import com.activeviam.database.datastore.api.transaction.ITransactionManager;
import com.activeviam.database.datastore.internal.IInternalDatastore;
import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.cfg.impl.RegistryInitializationConfig;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig.ParentType;
import com.activeviam.tech.core.api.agent.AgentException;
import com.activeviam.tech.observability.internal.memory.AMemoryStatistic;
import com.activeviam.tech.records.api.ICursor;
import com.activeviam.tech.records.api.IRecordReader;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestBranches extends ATestMemoryStatistic {

  private ApplicationInTests<IDatastore> monitoredApp;
  private ApplicationInTests<IInternalDatastore> monitoringApp;

  @BeforeAll
  public static void setupRegistry() {
    RegistryInitializationConfig.setupRegistry();
  }

  @BeforeEach
  public void setup() throws AgentException, DatastoreTransactionException {
    initializeApplication();

    final Path exportPath = generateMemoryStatistics();

    final AMemoryStatistic stats = loadMemoryStatFromFolder(exportPath);

    initializeMonitoringApplication(stats);

    IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);
    Assertions.assertThat(pivot).isNotNull();
  }

  private void initializeApplication() throws DatastoreTransactionException {
    this.monitoredApp = createMicroApplicationWithKeepAllEpochPolicy();

    final ITransactionManager transactionManager =
        this.monitoredApp.getDatabase().getTransactionManager();

    transactionManager.startTransactionOnBranch("branch1", "A");
    IntStream.range(0, 10).forEach(i -> transactionManager.add("A", i, 0.));
    transactionManager.commitTransaction();

    transactionManager.startTransactionOnBranch("branch2", "A");
    IntStream.range(10, 20).forEach(i -> transactionManager.add("A", i, 0.));
    transactionManager.commitTransaction();

    transactionManager.startTransactionFromBranch("subbranch", "branch2", "A");
    IntStream.range(20, 30).forEach(i -> transactionManager.add("A", i, 0.));
    transactionManager.commitTransaction();

    transactionManager.startTransactionOnBranch("branch1", "A");
    IntStream.range(10, 20).forEach(i -> transactionManager.add("A", i, 0.));
    transactionManager.commitTransaction();
  }

  private Path generateMemoryStatistics() {
    performGC();

    final MemoryAnalysisService analysisService =
        (MemoryAnalysisService)
            createService(this.monitoredApp.getDatabase(), this.monitoredApp.getManager());
    return analysisService.exportApplication("testBranches");
  }

  private void initializeMonitoringApplication(final AMemoryStatistic data) {
    final ManagerDescriptionConfig config = new ManagerDescriptionConfig();
    final IDatastoreSchemaDescriptionConfig schemaConfig =
        new MemoryAnalysisDatastoreDescriptionConfig();

    ApplicationInTests<IInternalDatastore> application =
        ApplicationInTests.builder()
            .withDatastore(schemaConfig.datastoreSchemaDescription())
            .withManager(config.managerDescription())
            .build();
    resources.register(application).start();

    this.monitoringApp = application;

    ATestMemoryStatistic.feedMonitoringApplication(
        application.getDatabase(), List.of(data), "testBranches");
  }

  @Test
  public void testExpectedBranches() {
    final Set<String> branches = retrieveBranches();

    Assertions.assertThat(branches)
        .containsExactlyInAnyOrder("master", "branch1", "branch2", "subbranch");
  }

  @Test
  public void testExpectedEpochs() {
    final Multimap<String, Long> epochsPerBranch = retrieveEpochsPerBranch();

    Assertions.assertThat(epochsPerBranch.get("master")).hasSize(1);
    Assertions.assertThat(epochsPerBranch.get("branch1")).hasSize(2);
    Assertions.assertThat(epochsPerBranch.get("branch2")).hasSize(1);
    Assertions.assertThat(epochsPerBranch.get("subbranch")).hasSize(1);

    Assertions.assertThat(epochsPerBranch.values()).doesNotHaveDuplicates();
  }

  @Test
  public void testMasterHasNoRecordChunks() {
    final Set<Long> recordChunks = retrieveRecordChunks();
    final Multimap<String, Long> chunksPerBranch = retrieveChunksPerBranch(recordChunks);

    Assertions.assertThat(chunksPerBranch.get("master")).isEmpty();
  }

  @Test
  public void testNonEmptyRecordChunks() {
    final Set<Long> recordChunks = retrieveRecordChunks();
    final Multimap<String, Long> chunksPerBranch = retrieveChunksPerBranch(recordChunks);

    Assertions.assertThat(chunksPerBranch.get("branch1")).isNotEmpty();
    Assertions.assertThat(chunksPerBranch.get("branch2")).isNotEmpty();
    Assertions.assertThat(chunksPerBranch.get("subbranch")).isNotEmpty();
  }

  @Test
  public void testRecordChunksInclusions() {
    final Set<Long> recordChunks = retrieveRecordChunks();
    final Multimap<String, Long> chunksPerBranch = retrieveChunksPerBranch(recordChunks);

    Assertions.assertThat(chunksPerBranch.get("branch1"))
        .containsAll(chunksPerBranch.get("master"));

    Assertions.assertThat(chunksPerBranch.get("branch2"))
        .containsAll(chunksPerBranch.get("master"));

    Assertions.assertThat(chunksPerBranch.get("subbranch"))
        .containsAll(chunksPerBranch.get("branch2"));
  }

  protected Set<String> retrieveBranches() {
    ListQuery query =
        this.monitoringApp
            .getDatabase()
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.VERSION_STORE)
            .withoutCondition()
            .withAliasedFields(AliasedField.fromFieldName(DatastoreConstants.VERSION__BRANCH_NAME))
            .toQuery();

    try (final ICursor cursor =
        this.monitoringApp
            .getDatabase()
            .getHead("master")
            .getQueryRunner()
            .listQuery(query)
            .run()) {

      return StreamSupport.stream(cursor.spliterator(), false)
          .map(c -> (String) c.read(0))
          .collect(Collectors.toSet());
    }
  }

  protected Multimap<String, Long> retrieveEpochsPerBranch() {
    ListQuery query =
        this.monitoringApp
            .getDatabase()
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.VERSION_STORE)
            .withoutCondition()
            .withAliasedFields(
                AliasedField.fromFieldName(DatastoreConstants.VERSION__BRANCH_NAME),
                AliasedField.fromFieldName(DatastoreConstants.VERSION__EPOCH_ID))
            .toQuery();
    try (final ICursor cursor =
        this.monitoringApp
            .getDatabase()
            .getHead("master")
            .getQueryRunner()
            .listQuery(query)
            .run()) {

      final Multimap<String, Long> epochsPerBranch = HashMultimap.create();
      for (final IRecordReader reader : cursor) {
        final String branch = (String) reader.read(0);
        final long epochId = reader.readLong(1);
        epochsPerBranch.put(branch, epochId);
      }

      return epochsPerBranch;
    }
  }

  protected Set<Long> retrieveRecordChunks() {
    ListQuery query =
        this.monitoringApp
            .getDatabase()
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.CHUNK_STORE)
            .withCondition(
                BaseConditions.equal(
                    FieldPath.of(DatastoreConstants.OWNER__COMPONENT), ParentType.RECORDS))
            .withAliasedFields(AliasedField.fromFieldName(DatastoreConstants.CHUNK_ID))
            .toQuery();
    try (final ICursor cursor =
        this.monitoringApp
            .getDatabase()
            .getHead("master")
            .getQueryRunner()
            .listQuery(query)
            .run()) {

      return StreamSupport.stream(cursor.spliterator(), false)
          .map(c -> c.readLong(0))
          .collect(Collectors.toSet());
    }
  }

  protected Multimap<String, Long> retrieveChunksPerBranch(final Collection<Long> chunkSet) {
    ListQuery query =
        this.monitoringApp
            .getDatabase()
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.CHUNK_STORE)
            .withCondition(
                BaseConditions.in(FieldPath.of(DatastoreConstants.CHUNK_ID), chunkSet.toArray()))
            .withAliasedFields(
                AliasedField.fromFieldName(DatastoreConstants.VERSION__EPOCH_ID),
                AliasedField.fromFieldName(DatastoreConstants.CHUNK_ID))
            .toQuery();
    try (final ICursor cursor =
        this.monitoringApp
            .getDatabase()
            .getHead("master")
            .getQueryRunner()
            .listQuery(query)
            .run()) {

      final Map<Long, String> epochToBranch = retrieveEpochToBranchMapping();

      final Multimap<String, Long> chunksPerEpoch = HashMultimap.create();
      for (final IRecordReader reader : cursor) {
        final String branch = epochToBranch.get(reader.readLong(0));
        final long chunkId = reader.readLong(1);
        chunksPerEpoch.put(branch, chunkId);
      }

      return chunksPerEpoch;
    }
  }

  protected Map<Long, String> retrieveEpochToBranchMapping() {
    ListQuery query =
        this.monitoringApp
            .getDatabase()
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.VERSION_STORE)
            .withoutCondition()
            .withAliasedFields(
                AliasedField.fromFieldName(DatastoreConstants.VERSION__BRANCH_NAME),
                AliasedField.fromFieldName(DatastoreConstants.VERSION__EPOCH_ID))
            .toQuery();
    try (final ICursor cursor =
        this.monitoringApp
            .getDatabase()
            .getHead("master")
            .getQueryRunner()
            .listQuery(query)
            .run()) {

      final Map<Long, String> epochToBranch = new HashMap<>();
      for (final IRecordReader reader : cursor) {
        final long epochId = reader.readLong(1);
        final String branch = (String) reader.read(0);
        epochToBranch.put(epochId, branch);
      }

      return epochToBranch;
    }
  }
}
