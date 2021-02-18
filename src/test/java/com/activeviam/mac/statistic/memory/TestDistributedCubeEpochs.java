/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import static com.quartetfs.fwk.util.TestUtils.waitAndAssert;

import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.statistic.memory.descriptions.DistributedApplicationDescription;
import com.activeviam.mac.statistic.memory.junit.RegistrySetupExtension;
import com.activeviam.mac.statistic.memory.visitor.impl.DistributedEpochView;
import com.activeviam.mac.statistic.memory.visitor.impl.EpochView;
import com.activeviam.mac.statistic.memory.visitor.impl.RegularEpochView;
import com.activeviam.properties.impl.ActiveViamProperty;
import com.activeviam.properties.impl.ActiveViamPropertyExtension;
import com.activeviam.properties.impl.ActiveViamPropertyExtension.ActiveViamPropertyExtensionBuilder;
import com.qfs.junit.LocalResourcesExtension;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.pivot.impl.MultiVersionDistributedActivePivot;
import com.qfs.store.query.ICursor;
import com.qfs.util.impl.QfsFileTestUtils;
import com.quartetfs.fwk.AgentException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(RegistrySetupExtension.class)
public class TestDistributedCubeEpochs {

  @RegisterExtension
  protected static ActiveViamPropertyExtension propertyExtension =
      new ActiveViamPropertyExtensionBuilder()
          .withProperty(ActiveViamProperty.ACTIVEVIAM_TEST_PROPERTY, true)
          .build();

  @RegisterExtension
  protected final LocalResourcesExtension resources = new LocalResourcesExtension();

  protected static Path tempDir =
      QfsFileTestUtils.createTempDirectory(TestAggregateProvidersBookmark.class);

  protected Application monitoredApplication;
  protected Application monitoringApplication;

  @BeforeEach
  public void setup(TestInfo testInfo) throws AgentException {
    monitoredApplication = MonitoringTestUtils.setupApplication(
        new DistributedApplicationDescription(testInfo.getDisplayName()),
        resources,
        (datastore, manager) -> {
          final var queryCubeA =
              ((MultiVersionDistributedActivePivot) manager.getActivePivots().get("QueryCubeA"));
          final var queryCubeB =
              ((MultiVersionDistributedActivePivot) manager.getActivePivots().get("QueryCubeB"));
          awaitEpochOnCubes(List.of(queryCubeA, queryCubeB), 2);

          // epoch 1
          datastore.edit(transactionManager -> IntStream.range(0, 10)
              .forEach(i -> transactionManager.add("A", i, (double) i)));
          awaitEpochOnCubes(List.of(queryCubeA, queryCubeB), 3);

          // emulate commits on the query cubes at a greater epoch that does not exist in the
          // datastore
          // produces 5 distributed epochs
          for (int i = 0; i < 5; ++i) {
            queryCubeA.removeMembersFromCube(Collections.emptySet(), 0, false);
          }

          // produces 1 distributed epoch
          queryCubeB.removeMembersFromCube(Collections.emptySet(), 0, false);
        });

    // todo vlg: use export application after 5.9.5
    final Path exportPath = MonitoringTestUtils.exportMostRecentVersion(
        monitoredApplication.getDatastore(),
        monitoredApplication.getManager(),
        tempDir,
        this.getClass().getSimpleName());

    final IMemoryStatistic stats = MonitoringTestUtils.loadMemoryStatFromFolder(exportPath);
    monitoringApplication = MonitoringTestUtils.setupMonitoringApplication(stats, resources);
  }

  private void awaitEpochOnCubes(
      final List<MultiVersionDistributedActivePivot> cubes, long epochId) {
    waitAndAssert(
        1,
        TimeUnit.MINUTES,
        () -> SoftAssertions.assertSoftly(
            assertions -> {
              for (final var cube : cubes) {
                assertions.assertThat(cube.getHead().getEpochId())
                    .as(cube.getId())
                    .isEqualTo(epochId);
              }
            }));
  }

  @Test
  public void testExpectedViewEpochs() {
    final Set<EpochView> viewEpochIds = retrieveViewEpochIds();
    Assertions.assertThat(viewEpochIds)
        .containsExactlyInAnyOrder(
            new RegularEpochView(1L),
            new DistributedEpochView("QueryCubeA", getHeadEpochId("QueryCubeA")),
            new DistributedEpochView("QueryCubeB", getHeadEpochId("QueryCubeB")));
  }

  private long getHeadEpochId(String cubeName) {
    return monitoredApplication
        .getManager()
        .getActivePivots()
        .get(cubeName)
        .getMostRecentVersion()
        .getEpochId();
  }

  protected Set<EpochView> retrieveViewEpochIds() {
    final ICursor cursor = monitoringApplication.getDatastore()
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
