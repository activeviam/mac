/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.scenarios;

import static com.activeviam.mac.statistic.memory.ATestMemoryStatistic.performGC;

import com.activeviam.activepivot.core.datastore.api.builder.StartBuilding;
import com.activeviam.activepivot.core.impl.internal.utils.ApplicationInTests;
import com.activeviam.activepivot.core.intf.api.cube.IActivePivotManager;
import com.activeviam.activepivot.core.intf.api.cube.IActivePivotVersion;
import com.activeviam.activepivot.core.intf.api.description.IActivePivotManagerDescription;
import com.activeviam.activepivot.core.intf.api.location.IHierarchicalMapping;
import com.activeviam.activepivot.server.impl.private_.observability.memory.MemoryAnalysisService;
import com.activeviam.activepivot.server.impl.private_.observability.memory.MemoryStatisticSerializerUtil;
import com.activeviam.activepivot.server.intf.api.observability.IMemoryAnalysisService;
import com.activeviam.database.api.types.ILiteralType;
import com.activeviam.database.datastore.api.IDatastore;
import com.activeviam.database.datastore.api.description.IDatastoreSchemaDescription;
import com.activeviam.database.datastore.internal.IInternalDatastore;
import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.cfg.impl.RegistryInitializationConfig;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.mac.statistic.memory.ATestMemoryStatistic;
import com.activeviam.tech.core.api.exceptions.ActiveViamRuntimeException;
import com.activeviam.tech.dictionaries.api.IDictionary;
import com.activeviam.tech.dictionaries.internal.impl.NullableDictionary;
import com.activeviam.tech.observability.internal.memory.AMemoryStatistic;
import com.activeviam.tech.test.internal.junit.resources.Resources;
import com.activeviam.tech.test.internal.junit.resources.ResourcesExtension;
import com.activeviam.tech.test.internal.junit.resources.ResourcesHolder;
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

/** The scenario this test produces has a cube level {@code id} with a nullable dictionary. */
@ExtendWith({ResourcesExtension.class})
public class TestNullableLevelDictionary {

  protected static final Path TEMP_DIRECTORY =
      FileTestUtil.createTempDirectory(TestNullableLevelDictionary.class);
  protected static final int RECORD_COUNT = 10;
  @Resources public ResourcesHolder resources;
  protected IDatastore datastore;
  protected IActivePivotManager manager;
  protected Path statisticsPath;

  @BeforeAll
  public static void setupRegistry() {
    RegistryInitializationConfig.setupRegistry();
  }

  @BeforeEach
  public void setupAndExportApplication() {
    final IDatastoreSchemaDescription datastoreSchemaDescription = datastoreSchema();
    final IActivePivotManagerDescription managerDescription =
        managerDescription(datastoreSchemaDescription);

    final ApplicationInTests<IDatastore> application =
        ApplicationInTests.builder()
            .withDatastore(datastoreSchemaDescription)
            .withManager(managerDescription)
            .build();

    this.datastore = application.getDatabase();

    this.manager = application.getManager();
    resources.register(application).start();

    fillApplication();
    performGC();
    exportApplicationMemoryStatistics();
  }

  protected IDatastoreSchemaDescription datastoreSchema() {
    return StartBuilding.datastoreSchema()
        .withStore(
            StartBuilding.store()
                .withStoreName("Store")
                .withNullableField("id", ILiteralType.OBJECT)
                .asKeyField()
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
            .withCube(StartBuilding.cube("Cube").withSingleLevelDimension("id").build())
            .build();

    return managerDescription;
  }

  protected void fillApplication() {
    this.datastore.edit(
        transactionManager -> {
          for (int i = 0; i < RECORD_COUNT; ++i) {
            transactionManager.add("Store", i);
          }
        });
  }

  protected void exportApplicationMemoryStatistics() {
    final IMemoryAnalysisService analysisService =
        new MemoryAnalysisService(this.datastore, this.manager, TEMP_DIRECTORY);
    this.statisticsPath = analysisService.exportMostRecentVersion("memoryStats");
  }

  @Test
  public void testLevelHasNullableDictionary() {
    final IActivePivotVersion cube = this.manager.getActivePivots().get("Cube").getHead();
    final IHierarchicalMapping mapping = cube.getHierarchicalMapping();

    final int hierarchyCoord = mapping.getCoordinate(1, 1);
    final IDictionary<?> dictionary = mapping.getDictionaries()[hierarchyCoord];

    Assertions.assertEquals(
        "id", mapping.getHierarchy(mapping.getHierarchyIndex(hierarchyCoord)).getName());
    Assertions.assertTrue(dictionary instanceof NullableDictionary);
  }

  @Test
  public void testStatisticLoading() throws IOException {
    final Collection<AMemoryStatistic> memoryStatistics = loadMemoryStatistic(this.statisticsPath);

    final IInternalDatastore analysisDatastore = createAnalysisDatastore();
    Assertions.assertDoesNotThrow(
        () -> loadStatisticsIntoDatastore(memoryStatistics, analysisDatastore));
  }

  protected IInternalDatastore createAnalysisDatastore() {
    final IDatastoreSchemaDescription desc =
        new MemoryAnalysisDatastoreDescriptionConfig().datastoreSchemaDescription();
    final IActivePivotManagerDescription manager =
        new ManagerDescriptionConfig().managerDescription();
    return (IInternalDatastore)
        ApplicationInTests.builder().withDatastore(desc).withManager(manager).build().getDatabase();
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

  protected void loadStatisticsIntoDatastore(
      final Collection<? extends AMemoryStatistic> statistics,
      final IInternalDatastore analysisDatastore) {
    ATestMemoryStatistic.feedMonitoringApplication(analysisDatastore, statistics, "test");
  }
}
