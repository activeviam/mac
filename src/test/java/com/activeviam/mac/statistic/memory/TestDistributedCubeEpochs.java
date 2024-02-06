/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import static com.qfs.distribution.impl.DistributionUtil.stopDistribution;
import static org.assertj.core.api.Assertions.assertThat;

import com.activeviam.database.api.query.AliasedField;
import com.activeviam.database.api.query.ListQuery;
import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.mac.statistic.memory.visitor.impl.DistributedEpochView;
import com.activeviam.mac.statistic.memory.visitor.impl.EpochView;
import com.activeviam.mac.statistic.memory.visitor.impl.RegularEpochView;
import com.activeviam.pivot.utils.ApplicationInTests;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.pivot.impl.MultiVersionDistributedActivePivot;
import com.qfs.pivot.monitoring.impl.MemoryAnalysisService;
import com.qfs.server.cfg.IDatastoreSchemaDescriptionConfig;
import com.qfs.store.IDatastore;
import com.qfs.store.query.ICursor;
import com.quartetfs.biz.pivot.IMultiVersionActivePivot;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.contributions.impl.ClasspathContributionProvider;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class TestDistributedCubeEpochs extends ATestMemoryStatistic {

  private ApplicationInTests<IDatastore> monitoredApp;
  private ApplicationInTests<IDatastore> monitoringApp;

  @BeforeAll
  public static void setupRegistry() {
    Registry.setContributionProvider(new ClasspathContributionProvider());
  }

  @BeforeEach
  public void setup() throws AgentException {
    initializeApplication();

    final Path exportPath = generateMemoryStatistics();
    final IMemoryStatistic statistics = loadMemoryStatFromFolder(exportPath);

    initializeMonitoringApplication(statistics);

    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);
    assertThat(pivot).isNotNull();
  }

  @AfterEach
  public void tearDown() {
    monitoringApp.close();
    stopDistribution(monitoredApp.getManager());
  }

  private void initializeApplication() {
    // In JUnit5, we can also use TestInfo to complete the cluster name with the test name
    this.monitoredApp = createDistributedApplicationWithKeepAllEpochPolicy("distributed-epochs");

    final var queryCubeA =
        ((MultiVersionDistributedActivePivot)
            this.monitoredApp.getManager().getActivePivots().get("QueryCubeA"));
    final var queryCubeB =
        ((MultiVersionDistributedActivePivot)
            this.monitoredApp.getManager().getActivePivots().get("QueryCubeB"));
    final var dataCube = this.monitoredApp.getManager().getActivePivots().get("Data");
    // epoch 1
    this.monitoredApp
        .getDatabase()
        .edit(
            transactionManager ->
                IntStream.range(0, 10).forEach(i -> transactionManager.add("A", i, (double) i)));

    dataCube.awaitNotifications();
    queryCubeA.awaitNotifications();
    queryCubeB.awaitNotifications();

    // emulate commits on the query cubes at a greater epoch that does not exist in the datastore
    // produces 5 distributed epochs
    for (int i = 0; i < 5; ++i) {
      queryCubeA.removeMembersFromCube(Collections.emptySet(), 0, false);
      queryCubeA.awaitNotifications();
      dataCube.awaitNotifications();
    }

    // produces 1 distributed epoch
    queryCubeB.removeMembersFromCube(Collections.emptySet(), 0, false);
    queryCubeB.awaitNotifications();
    dataCube.awaitNotifications();
  }

  private Path generateMemoryStatistics() {
    this.monitoredApp.getDatabase().getEpochManager().forceDiscardEpochs(node -> true);
    performGC();

    final MemoryAnalysisService analysisService =
        (MemoryAnalysisService)
            createService(this.monitoredApp.getDatabase(), this.monitoredApp.getManager());
    return analysisService.exportApplication("testEpochs");
  }

  private void initializeMonitoringApplication(final IMemoryStatistic data) {
    final ManagerDescriptionConfig config = new ManagerDescriptionConfig();
    final IDatastoreSchemaDescriptionConfig schemaConfig =
        new MemoryAnalysisDatastoreDescriptionConfig();

    this.monitoringApp =
        ApplicationInTests.builder()
            .withDatastore(schemaConfig.datastoreSchemaDescription())
            .withManager(config.managerDescription())
            .build();

    resources.register(monitoringApp).start();

    ATestMemoryStatistic.feedMonitoringApplication(
        monitoringApp.getDatabase(), List.of(data), "testDistributedCubeEpochs");
  }

  @Disabled("See https://activeviam.atlassian.net/browse/PIVOT-7689")
  @Test
  public void testExpectedViewEpochs() {
    final Set<EpochView> viewEpochIds = retrieveViewEpochIds();

    assertThat(viewEpochIds)
        .containsExactlyInAnyOrder(
            new RegularEpochView(getHeadEpochId("Data")),
            new DistributedEpochView("QueryCubeA", getHeadEpochId("QueryCubeA")),
            new DistributedEpochView("QueryCubeB", getHeadEpochId("QueryCubeB")));
  }

  private long getHeadEpochId(String queryCube) {
    return this.monitoredApp
        .getManager()
        .getActivePivots()
        .get(queryCube)
        .getMostRecentVersion()
        .getEpochId();
  }

  protected Set<EpochView> retrieveViewEpochIds() {
    ListQuery query =
        this.monitoringApp
            .getDatabase()
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.EPOCH_VIEW_STORE)
            .withoutCondition()
            .withAliasedFields(
                AliasedField.fromFieldName(DatastoreConstants.EPOCH_VIEW__VIEW_EPOCH_ID))
            .toQuery();
    try (final ICursor cursor =
        this.monitoringApp
            .getDatabase()
            .getHead("master")
            .getQueryRunner()
            .listQuery(query)
            .run()) {

      return StreamSupport.stream(cursor.spliterator(), false)
          .map(c -> (EpochView) c.read(0))
          .collect(Collectors.toSet());
    }
  }
}
