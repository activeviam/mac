/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.scenarios;

import com.activeviam.activepivot.core.datastore.api.builder.StartBuilding;
import com.activeviam.activepivot.core.impl.internal.monitoring.memory.impl.OnHeapPivotMemoryQuantifierPlugin;
import com.activeviam.activepivot.core.impl.internal.test.util.PivotTestUtils;
import com.activeviam.activepivot.core.impl.internal.utils.ApplicationInTests;
import com.activeviam.activepivot.core.intf.api.cube.IActivePivotManager;
import com.activeviam.activepivot.core.intf.api.description.IActivePivotManagerDescription;
import com.activeviam.activepivot.server.impl.private_.observability.memory.MemoryAnalysisService;
import com.activeviam.activepivot.server.impl.private_.observability.memory.MemoryStatisticSerializerUtil;
import com.activeviam.activepivot.server.intf.api.observability.IMemoryAnalysisService;
import com.activeviam.database.api.types.ILiteralType;
import com.activeviam.database.datastore.api.IDatastore;
import com.activeviam.database.datastore.api.description.IDatastoreSchemaDescription;
import com.activeviam.database.datastore.internal.IInternalDatastore;
import com.activeviam.database.datastore.internal.builder.impl.UnitTestDatastoreBuilder;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.mac.statistic.memory.ATestMemoryStatistic;
import com.activeviam.tech.core.api.agent.AgentException;
import com.activeviam.tech.core.api.exceptions.ActiveViamRuntimeException;
import com.activeviam.tech.observability.internal.memory.AMemoryStatistic;
import com.activeviam.tech.test.internal.junit.resources.ResourcesExtension;
import com.activeviam.tech.test.internal.util.FileTestUtil;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * The scenario this test produces created a situation where the dictionary used by the point index
 * of the aggregate store of the cube would leak into subsequent chunks.
 */
@ExtendWith({ResourcesExtension.class})
public class TestBitmapAggregateProviderWithVectorPrimitiveMeasure extends ATestMemoryStatistic {

  protected static final Path TEMP_DIRECTORY =
      FileTestUtil.createTempDirectory(TestBitmapAggregateProviderWithVectorPrimitiveMeasure.class);
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
    final Collection<AMemoryStatistic> memoryStatistics = loadMemoryStatistic(this.statisticsPath);

    final IInternalDatastore analysisDatastore = createAnalysisDatastore();
    Assertions.assertDoesNotThrow(
        () ->
            ATestMemoryStatistic.feedMonitoringApplication(
                analysisDatastore, memoryStatistics, "test"));
  }

  protected IInternalDatastore createAnalysisDatastore() {
    final IDatastoreSchemaDescription desc =
        new MemoryAnalysisDatastoreDescriptionConfig().datastoreSchemaDescription();
    final IInternalDatastore datastore =
        new UnitTestDatastoreBuilder().setSchemaDescription(desc).build();
    this.resources.register(datastore);
    return datastore;
  }

  protected Collection<AMemoryStatistic> loadMemoryStatistic(final Path path) throws IOException {
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
