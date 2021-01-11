/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import static com.quartetfs.fwk.util.TestUtils.waitAndAssert;
import static org.assertj.core.api.Assertions.assertThat;

import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.statistic.memory.visitor.impl.DistributedEpochView;
import com.activeviam.mac.statistic.memory.visitor.impl.EpochView;
import com.activeviam.mac.statistic.memory.visitor.impl.RegularEpochView;
import com.activeviam.pivot.builders.StartBuilding;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.pivot.impl.MultiVersionDistributedActivePivot;
import com.qfs.pivot.monitoring.impl.MemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.qfs.store.query.ICursor;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.IMultiVersionActivePivot;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.contributions.impl.ClasspathContributionProvider;
import com.quartetfs.fwk.impl.Pair;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDistributedCubeEpochs extends ATestMemoryStatistic {

  private Pair<IDatastore, IActivePivotManager> monitoredApp;
  private Pair<IDatastore, IActivePivotManager> monitoringApp;

  @BeforeClass
  public static void setupRegistry() {
    Registry.setContributionProvider(new ClasspathContributionProvider());
  }

  @Before
  public void setup() throws AgentException {
    initializeApplication();

    final Path exportPath = generateMemoryStatistics();
    final IMemoryStatistic statistics = loadMemoryStatFromFolder(exportPath);

    initializeMonitoringApplication(statistics);

    final IMultiVersionActivePivot pivot =
        monitoringApp.getRight().getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);
    assertThat(pivot).isNotNull();
  }

  private void initializeApplication() {
    // In JUnit5, we can also use TestInfo to complete the cluster name with the test name
    monitoredApp = createDistributedApplicationWithKeepAllEpochPolicy("distributed-epochs");
    resources.register(monitoredApp.getRight()::stop);
    resources.register(monitoredApp.getLeft()::stop);

    final var queryCubeA =
        ((MultiVersionDistributedActivePivot)
            monitoredApp.getRight().getActivePivots().get("QueryCubeA"));
    final var queryCubeB =
        ((MultiVersionDistributedActivePivot)
            monitoredApp.getRight().getActivePivots().get("QueryCubeB"));
    awaitEpochOnCubes(List.of(queryCubeA, queryCubeB), 2);

    // epoch 1
    monitoredApp
        .getLeft()
        .edit(
            transactionManager -> {
              IntStream.range(0, 10).forEach(i -> transactionManager.add("A", i, (double) i));
            });
    awaitEpochOnCubes(List.of(queryCubeA, queryCubeB), 3);

    // emulate commits on the query cubes at a greater epoch that does not exist in the datastore
    // produces 5 distributed epochs
    for (int i = 0; i < 5; ++i) {
      queryCubeA.removeMembersFromCube(Collections.emptySet(), 0, false);
    }

    // produces 1 distributed epoch
    queryCubeB.removeMembersFromCube(Collections.emptySet(), 0, false);
  }

  private void awaitEpochOnCubes(
      final List<MultiVersionDistributedActivePivot> cubes, long epochId) {
    waitAndAssert(
        1,
        TimeUnit.MINUTES,
        () -> {
          SoftAssertions.assertSoftly(
              assertions -> {
                for (final var cube : cubes) {
                  assertions
                      .assertThat(cube.getHead().getEpochId())
                      .as(cube.getId())
                      .isEqualTo(epochId);
                }
              });
        });
  }

  private Path generateMemoryStatistics() {
    monitoredApp.getLeft().getEpochManager().forceDiscardEpochs(node -> true);
    performGC();

    final MemoryAnalysisService analysisService =
        (MemoryAnalysisService) createService(monitoredApp.getLeft(), monitoredApp.getRight());
    return analysisService.exportMostRecentVersion("testEpochs");
    //    return analysisService.exportApplication("testEpochs"); // todo vlg: update the test to
    // use this when export is fixed PIVOT-4460
  }

  private void initializeMonitoringApplication(final IMemoryStatistic data) throws AgentException {
    final ManagerDescriptionConfig config = new ManagerDescriptionConfig();
    final IDatastore monitoringDatastore =
        resources.create(
            () ->
                StartBuilding.datastore().setSchemaDescription(config.schemaDescription()).build());

    final IActivePivotManager manager =
        StartBuilding.manager()
            .setDescription(config.managerDescription())
            .setDatastoreAndPermissions(monitoringDatastore)
            .buildAndStart();
    resources.register(manager::stop);
    monitoringApp = new Pair<>(monitoringDatastore, manager);

    ATestMemoryStatistic.feedMonitoringApplication(
        monitoringDatastore, List.of(data), "testDistributedCubeEpochs");
  }

  @Test
  public void testExpectedViewEpochs() {
    final Set<EpochView> viewEpochIds = retrieveViewEpochIds();

    assertThat(viewEpochIds)
        .containsExactlyInAnyOrder(
            new RegularEpochView(1L),
            new DistributedEpochView("QueryCubeA", getHeadEpochId("QueryCubeA")),
            new DistributedEpochView("QueryCubeB", getHeadEpochId("QueryCubeB")));
  }

  private long getHeadEpochId(String queryCubeA) {
    return monitoredApp
        .getRight()
        .getActivePivots()
        .get(queryCubeA)
        .getMostRecentVersion()
        .getEpochId();
  }

  protected Set<EpochView> retrieveViewEpochIds() {
    final ICursor cursor =
        monitoringApp
            .getLeft()
            .getHead()
            .getQueryRunner()
            .forStore(DatastoreConstants.EPOCH_VIEW_STORE)
            .withoutCondition()
            .selecting(DatastoreConstants.EPOCH_VIEW__VIEW_EPOCH_ID)
            .onCurrentThread()
            .run();

    return StreamSupport.stream(cursor.spliterator(), false)
        .map(c -> (EpochView) c.read(0))
        .collect(Collectors.toSet());
  }
}
