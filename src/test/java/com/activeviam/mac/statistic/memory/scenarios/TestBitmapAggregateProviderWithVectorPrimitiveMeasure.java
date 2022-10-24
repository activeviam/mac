/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.scenarios;

import com.activeviam.builders.StartBuilding;
import com.activeviam.fwk.ActiveViamRuntimeException;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.mac.statistic.memory.ATestMemoryStatistic;
import com.activeviam.pivot.utils.ApplicationInTests;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.junit.LocalResourcesExtension;
import com.qfs.literal.ILiteralType;
import com.qfs.monitoring.memory.impl.OnHeapPivotMemoryQuantifierPlugin;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.pivot.monitoring.impl.MemoryAnalysisService;
import com.qfs.pivot.monitoring.impl.MemoryStatisticSerializerUtil;
import com.qfs.service.monitoring.IMemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.qfs.store.build.impl.UnitTestDatastoreBuilder;
import com.qfs.util.impl.QfsFileTestUtils;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.test.util.PivotTestUtils;
import com.quartetfs.fwk.AgentException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.stream.Collectors;
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

  protected static final Path TEMP_DIRECTORY =
      QfsFileTestUtils.createTempDirectory(
          TestBitmapAggregateProviderWithVectorPrimitiveMeasure.class);
  protected static final int RECORD_COUNT = 100;
  @RegisterExtension protected LocalResourcesExtension resources = new LocalResourcesExtension();
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

    final ApplicationInTests<IDatastore> application =
        ApplicationInTests.builder()
            .withDatastore(datastoreSchemaDescription)
            .withManager(managerDescription)
            .build();

    this.resources.register(application).start();
    this.datastore = application.getDatabase();
    this.manager = application.getManager();

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

    return StartBuilding.managerDescription()
        .withSchema()
        .withSelection(
            StartBuilding.selection(datastoreSchema).fromBaseStore("Store").withAllFields().build())
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
  }

  protected void fillApplication() {
    this.datastore.edit(
        transactionManager -> {
          for (int i = 0; i < RECORD_COUNT; ++i) {
            transactionManager.add("Store", i, new double[] {i, -i});
          }
        });
  }

  protected void exportApplicationMemoryStatistics() {
    final IMemoryAnalysisService analysisService =
        new MemoryAnalysisService(
            this.datastore,
            this.manager,
            TestBitmapAggregateProviderWithVectorPrimitiveMeasure.TEMP_DIRECTORY);
    this.statisticsPath = analysisService.exportMostRecentVersion("memoryStats");
  }

  @Test
  public void testLoading() throws IOException {
    final Collection<IMemoryStatistic> memoryStatistics = loadMemoryStatistic(this.statisticsPath);

    final IDatastore analysisDatastore = createAnalysisDatastore();
    Assertions.assertDoesNotThrow(
        () ->
            ATestMemoryStatistic.feedMonitoringApplication(
                analysisDatastore, memoryStatistics, "test"));
  }

  protected IDatastore createAnalysisDatastore() {
    final IDatastoreSchemaDescription desc =
        new MemoryAnalysisDatastoreDescriptionConfig().datastoreSchemaDescription();
    final IDatastore datastore = new UnitTestDatastoreBuilder().setSchemaDescription(desc).build();
    this.resources.register(datastore);
    return datastore;
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
