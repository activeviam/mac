/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.scenarios;

import com.activeviam.fwk.ActiveViamRuntimeException;
import com.activeviam.mac.memory.AnalysisDatastoreFeeder;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription;
import com.activeviam.mac.statistic.memory.ATestMemoryStatistic;
import com.activeviam.pivot.builders.StartBuilding;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.junit.LocalResourcesExtension;
import com.qfs.literal.ILiteralType;
import com.qfs.monitoring.memory.impl.OnHeapPivotMemoryQuantifierPlugin;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.pivot.monitoring.impl.MemoryAnalysisService;
import com.qfs.pivot.monitoring.impl.MemoryStatisticSerializerUtil;
import com.qfs.service.monitoring.IMemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.qfs.util.impl.QfsFileTestUtils;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.impl.ActivePivotDatastorePostProcessor;
import com.quartetfs.biz.pivot.impl.ActivePivotManagerBuilder;
import com.quartetfs.biz.pivot.test.util.PivotTestUtils;
import com.quartetfs.fwk.AgentException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * The scenario this test produces created a situation where the dictionary used by the point index
 * of the aggregate store of the cube would leak into subsequent chunks.
 */
public class TestBitmapAggregateProviderWithVectorPrimitiveMeasure extends ATestMemoryStatistic {

  @RegisterExtension protected LocalResourcesExtension resources = new LocalResourcesExtension();

  protected static final Path TEMP_DIRECTORY =
      QfsFileTestUtils.createTempDirectory(
          TestBitmapAggregateProviderWithVectorPrimitiveMeasure.class);

  protected static final int RECORD_COUNT = 100;

  protected IDatastore datastore;
  protected IActivePivotManager manager;
  protected Path statisticsPath;

  @BeforeAll
  public static void setupRegistry() {
    PivotTestUtils.setUpRegistry(OnHeapPivotMemoryQuantifierPlugin.class);
  }

  @BeforeEach
  public void setupAndExportApplication() throws AgentException {
    final IDatastoreSchemaDescription datastoreSchemaDescription = datastoreSchema();
    final IActivePivotManagerDescription managerDescription =
        managerDescription(datastoreSchemaDescription);

    datastore =
        StartBuilding.datastore()
            .setSchemaDescription(datastoreSchemaDescription)
            .addSchemaDescriptionPostProcessors(
                ActivePivotDatastorePostProcessor.createFrom(managerDescription))
            .build();

    manager =
        StartBuilding.manager()
            .setDescription(managerDescription)
            .setDatastoreAndPermissions(datastore)
            .buildAndStart();

    fillApplication();
    performGC();
    exportApplicationMemoryStatistics();
  }

  protected IDatastoreSchemaDescription datastoreSchema() {
    return StartBuilding.datastoreSchema()
        .withStore(
            StartBuilding.store()
                .withStoreName("Store")
                .withField("id", ILiteralType.INT)
                .asKeyField()
                .withVectorField("vectorMeasure", ILiteralType.DOUBLE)
                .withVectorBlockSize(4)
                .withChunkSize(2)
                .build())
        .build();
  }

  protected IActivePivotManagerDescription managerDescription(
      final IDatastoreSchemaDescription datastoreSchema) {
    final IActivePivotManagerDescription managerDescription =
        StartBuilding.managerDescription()
            .withSchema()
            .withSelection(
                StartBuilding.selection(datastoreSchema)
                    .fromBaseStore("Store")
                    .withAllFields()
                    .build())
            .withCube(
                StartBuilding.cube("Cube")
                    .withContributorsCount()
                    .withAggregatedMeasure()
                    .sum("vectorMeasure")
                    .withSingleLevelDimension("id")
                    .withPropertyName("id")
                    .withAggregateProvider()
                    .bitmap()
                    .build())
            .build();

    return ActivePivotManagerBuilder.postProcess(managerDescription, datastoreSchema);
  }

  protected void fillApplication() {
    datastore.edit(
        transactionManager -> {
          for (int i = 0; i < RECORD_COUNT; ++i) {
            transactionManager.add("Store", i, new double[] {i, -i});
          }
        });
  }

  protected void exportApplicationMemoryStatistics() {
    final IMemoryAnalysisService analysisService =
        new MemoryAnalysisService(
            datastore,
            manager,
            datastore.getEpochManager(),
            TestBitmapAggregateProviderWithVectorPrimitiveMeasure.TEMP_DIRECTORY);
    statisticsPath = analysisService.exportMostRecentVersion("memoryStats");
  }

  @AfterEach
  public void teardown() throws AgentException {
    manager.stop();
    datastore.stop();
  }

  @Test
  public void testLoading() throws IOException {
    final Collection<IMemoryStatistic> memoryStatistics = loadMemoryStatistic(statisticsPath);

    final IDatastore analysisDatastore = createAnalysisDatastore();
    Assertions.assertDoesNotThrow(
        () -> ATestMemoryStatistic.feedMonitoringApplication(analysisDatastore, memoryStatistics, "test"));
  }

  protected IDatastore createAnalysisDatastore() {
    final IDatastoreSchemaDescription desc = new MemoryAnalysisDatastoreDescription();
    return resources.create(StartBuilding.datastore().setSchemaDescription(desc)::build);
  }

  protected Collection<IMemoryStatistic> loadMemoryStatistic(final Path path) throws IOException {
    return Files.list(path)
        .map(
            file -> {
              try {
                return MemoryStatisticSerializerUtil.readStatisticFile(file.toFile());
              } catch (IOException exception) {
                throw new ActiveViamRuntimeException(exception);
              }
            })
        .collect(Collectors.toList());
  }

}
