/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.activeviam.copper.testing.CubeTester;
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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

  protected CubeTester tester;

  @BeforeEach
  public void setup() throws AgentException {
    monitoredApplication = MonitoringTestUtils.setupApplication(
        new DistributedApplicationDescription(),
        resources,
        (datastore, manager) -> {
          datastore.edit(transactionManager ->
              IntStream.range(0, 10).forEach(i ->
                  transactionManager.add("A", i, 0.)));

          // emulate commits on the query cubes at a greater epoch that does not exist in the datastore
          MultiVersionDistributedActivePivot queryCubeA =
              ((MultiVersionDistributedActivePivot) manager.getActivePivots().get("QueryCubeA"));

          // produces distributed epochs 1 to 5
          for (int i = 0; i < 5; ++i) {
            queryCubeA.removeMembersFromCube(Collections.emptySet(), 0, false);
          }

          MultiVersionDistributedActivePivot queryCubeB =
              ((MultiVersionDistributedActivePivot) manager.getActivePivots().get("QueryCubeB"));

          // produces distributed epoch 1
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

    tester = MonitoringTestUtils.createMonitoringCubeTester(monitoringApplication.getManager());
  }

  @Test
  public void testExpectedViewEpochs() {
    final Set<EpochView> viewEpochIds = retrieveViewEpochIds();

    Assertions.assertThat(viewEpochIds)
        .containsExactlyInAnyOrder(
            new RegularEpochView(1L),
            new DistributedEpochView("QueryCubeA", 5L),
            new DistributedEpochView("QueryCubeB", 1L));
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
