/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription.ParentType;
import com.activeviam.mac.statistic.memory.visitor.impl.FeedVisitor;
import com.activeviam.pivot.builders.StartBuilding;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.pivot.monitoring.impl.MemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.qfs.store.query.ICursor;
import com.qfs.store.record.IRecordReader;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.qfs.store.transaction.ITransactionManager;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.IMultiVersionActivePivot;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.contributions.impl.ClasspathContributionProvider;
import com.quartetfs.fwk.impl.Pair;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBranches extends ATestMemoryStatistic {

  private Pair<IDatastore, IActivePivotManager> monitoredApp;
  private Pair<IDatastore, IActivePivotManager> monitoringApp;

  @BeforeClass
  public static void setupRegistry() {
    Registry.setContributionProvider(new ClasspathContributionProvider());
  }

  @Before
  public void setup() throws AgentException, DatastoreTransactionException {
    initializeApplication();

    final Path exportPath = generateMemoryStatistics();

    final IMemoryStatistic stats = loadMemoryStatFromFolder(exportPath);

    initializeMonitoringApplication(stats);

    IMultiVersionActivePivot pivot =
        monitoringApp.getRight().getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);
    Assertions.assertThat(pivot).isNotNull();
  }

  private void initializeApplication() throws DatastoreTransactionException {
    monitoredApp = createMicroApplicationWithKeepAllEpochPolicy();

    final ITransactionManager transactionManager = monitoredApp.getLeft().getTransactionManager();

    transactionManager.startTransactionOnBranch("branch1", "A");
    IntStream.range(0, 10).forEach(i ->
        transactionManager.add("A", i, 0.));
    transactionManager.commitTransaction();

    transactionManager.startTransactionOnBranch("branch2", "A");
    IntStream.range(10, 20).forEach(i ->
        transactionManager.add("A", i, 0.));
    transactionManager.commitTransaction();

    transactionManager.startTransactionFromBranch("subbranch", "branch2", "A");
    IntStream.range(20, 30).forEach(i ->
        transactionManager.add("A", i, 0.));
    transactionManager.commitTransaction();

    transactionManager.startTransactionOnBranch("branch1", "A");
    IntStream.range(10, 20).forEach(i ->
        transactionManager.add("A", i, 0.));
    transactionManager.commitTransaction();
  }

  private Path generateMemoryStatistics() {
    performGC();

    final MemoryAnalysisService analysisService =
        (MemoryAnalysisService) createService(monitoredApp.getLeft(), monitoredApp.getRight());
    return analysisService.exportApplication("testBranches");
  }

  private void initializeMonitoringApplication(final IMemoryStatistic data) throws AgentException {
    ManagerDescriptionConfig config = new ManagerDescriptionConfig();
    final IDatastore monitoringDatastore =
        StartBuilding.datastore().setSchemaDescription(config.schemaDescription()).build();

    IActivePivotManager manager =
        StartBuilding.manager()
            .setDescription(config.managerDescription())
            .setDatastoreAndPermissions(monitoringDatastore)
            .buildAndStart();
    monitoringApp = new Pair<>(monitoringDatastore, manager);

    monitoringDatastore.edit(
        tm -> data.accept(new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "storeA")));
  }

  @After
  public void tearDown() throws AgentException {
    monitoringApp.getLeft().close();
    monitoringApp.getRight().stop();
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

    Assertions.assertThat(epochsPerBranch.get("master"))
        .hasSize(1);
    Assertions.assertThat(epochsPerBranch.get("branch1"))
        .hasSize(2);
    Assertions.assertThat(epochsPerBranch.get("branch2"))
        .hasSize(1);
    Assertions.assertThat(epochsPerBranch.get("subbranch"))
        .hasSize(1);

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
    final ICursor cursor = monitoringApp.getLeft().getHead().getQueryRunner()
        .forStore(DatastoreConstants.VERSION_STORE)
        .withoutCondition()
        .selecting(DatastoreConstants.VERSION__BRANCH_NAME)
        .onCurrentThread()
        .run();

    return StreamSupport.stream(cursor.spliterator(), false)
        .map(c -> (String) c.read(0))
        .collect(Collectors.toSet());
  }

  protected Multimap<String, Long> retrieveEpochsPerBranch() {
    final ICursor cursor = monitoringApp.getLeft().getHead().getQueryRunner()
        .forStore(DatastoreConstants.VERSION_STORE)
        .withoutCondition()
        .selecting(DatastoreConstants.VERSION__BRANCH_NAME, DatastoreConstants.VERSION__EPOCH_ID)
        .onCurrentThread()
        .run();

    final Multimap<String, Long> epochsPerBranch = HashMultimap.create();
    for (final IRecordReader reader : cursor) {
      final String branch = (String) reader.read(0);
      final long epochId = reader.readLong(1);
      epochsPerBranch.put(branch, epochId);
    }

    return epochsPerBranch;
  }

  protected Set<Long> retrieveRecordChunks() {
    final ICursor cursor = monitoringApp.getLeft().getHead().getQueryRunner()
        .forStore(DatastoreConstants.CHUNK_STORE)
        .withCondition(
            BaseConditions.Equal(DatastoreConstants.OWNER__COMPONENT, ParentType.RECORDS))
        .selecting(DatastoreConstants.CHUNK_ID)
        .onCurrentThread()
        .run();

    return StreamSupport.stream(cursor.spliterator(), false)
        .map(c -> c.readLong(0))
        .collect(Collectors.toSet());
  }

  protected Multimap<String, Long> retrieveChunksPerBranch(final Collection<Long> chunkSet) {
    final ICursor cursor = monitoringApp.getLeft().getHead().getQueryRunner()
        .forStore(DatastoreConstants.CHUNK_STORE)
        .withCondition(
            BaseConditions.In(DatastoreConstants.CHUNK_ID, chunkSet.toArray()))
        .selecting(DatastoreConstants.VERSION__EPOCH_ID, DatastoreConstants.CHUNK_ID)
        .onCurrentThread()
        .run();

    final Map<Long, String> epochToBranch = retrieveEpochToBranchMapping();

    final Multimap<String, Long> chunksPerEpoch = HashMultimap.create();
    for (final IRecordReader reader : cursor) {
      final String branch = epochToBranch.get(reader.readLong(0));
      final long chunkId = reader.readLong(1);
      chunksPerEpoch.put(branch, chunkId);
    }

    return chunksPerEpoch;
  }

  protected Map<Long, String> retrieveEpochToBranchMapping() {
    final ICursor cursor = monitoringApp.getLeft().getHead().getQueryRunner()
        .forStore(DatastoreConstants.VERSION_STORE)
        .withoutCondition()
        .selecting(DatastoreConstants.VERSION__BRANCH_NAME, DatastoreConstants.VERSION__EPOCH_ID)
        .onCurrentThread()
        .run();

    final Map<Long, String> epochToBranch = new HashMap<>();
    for (final IRecordReader reader : cursor) {
      final long epochId = reader.readLong(1);
      final String branch = (String) reader.read(0);
      epochToBranch.put(epochId, branch);
    }

    return epochToBranch;
  }
}
