/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.scenarios;

import static com.activeviam.mac.statistic.memory.ATestMemoryStatistic.performGC;

import com.activeviam.fwk.ActiveViamRuntimeException;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription;
import com.activeviam.mac.statistic.memory.ATestMemoryStatistic;
import com.activeviam.pivot.builders.StartBuilding;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.dic.IDictionary;
import com.qfs.dic.impl.NullableDictionary;
import com.qfs.junit.LocalResourcesExtension;
import com.qfs.literal.ILiteralType;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.pivot.monitoring.impl.MemoryAnalysisService;
import com.qfs.pivot.monitoring.impl.MemoryStatisticSerializerUtil;
import com.qfs.service.monitoring.IMemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.qfs.util.impl.QfsFileTestUtils;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.IActivePivotVersion;
import com.quartetfs.biz.pivot.cube.provider.IHierarchicalMapping;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.impl.ActivePivotManagerBuilder;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.contributions.impl.ClasspathContributionProvider;
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

/** The scenario this test produces has a cube level {@code id} with a nullable dictionary. */
public class TestNullableLevelDictionary {

  protected static final Path TEMP_DIRECTORY =
      QfsFileTestUtils.createTempDirectory(TestNullableLevelDictionary.class);
  protected static final int RECORD_COUNT = 10;
  @RegisterExtension protected LocalResourcesExtension resources = new LocalResourcesExtension();
  protected IDatastore datastore;
  protected IActivePivotManager manager;
  protected Path statisticsPath;

  @BeforeAll
  public static void setupRegistry() {
    Registry.setContributionProvider(new ClasspathContributionProvider());
  }

  @BeforeEach
  public void setupAndExportApplication() throws AgentException {
    final IDatastoreSchemaDescription datastoreSchemaDescription = datastoreSchema();
    final IActivePivotManagerDescription managerDescription =
        managerDescription(datastoreSchemaDescription);

    this.datastore =
        StartBuilding.datastore().setSchemaDescription(datastoreSchemaDescription).build();

    this.manager =
        StartBuilding.manager()
            .setDescription(managerDescription)
            .setDatastoreAndPermissions(this.datastore)
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

    return ActivePivotManagerBuilder.postProcess(managerDescription, datastoreSchema);
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
        new MemoryAnalysisService(
            this.datastore, this.manager, this.datastore.getEpochManager(), TEMP_DIRECTORY);
    this.statisticsPath = analysisService.exportMostRecentVersion("memoryStats");
  }

  @AfterEach
  public void teardown() throws AgentException {
    this.manager.stop();
    this.datastore.stop();
  }

  @Test
  public void testLevelHasNullableDictionary() {
    final IActivePivotVersion cube = this.manager.getActivePivots().get("Cube").getHead();
    final IHierarchicalMapping mapping = cube.getAggregateProvider().getHierarchicalMapping();

    final int hierarchyCoord = mapping.getCoordinate(1, 1);
    final IDictionary<?> dictionary = mapping.getDictionaries()[hierarchyCoord];

    Assertions.assertEquals(
        "id", mapping.getHierarchy(mapping.getHierarchyIndex(hierarchyCoord)).getName());
    Assertions.assertTrue(dictionary instanceof NullableDictionary);
  }

  @Test
  public void testStatisticLoading() throws IOException {
    final Collection<IMemoryStatistic> memoryStatistics = loadMemoryStatistic(this.statisticsPath);

    final IDatastore analysisDatastore = createAnalysisDatastore();
    Assertions.assertDoesNotThrow(
        () -> loadStatisticsIntoDatastore(memoryStatistics, analysisDatastore));
  }

  protected IDatastore createAnalysisDatastore() {
    final IDatastoreSchemaDescription desc = new MemoryAnalysisDatastoreDescription();
    return this.resources.create(StartBuilding.datastore().setSchemaDescription(desc)::build);
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

  protected void loadStatisticsIntoDatastore(
      final Collection<? extends IMemoryStatistic> statistics, final IDatastore analysisDatastore) {
    ATestMemoryStatistic.feedMonitoringApplication(analysisDatastore, statistics, "test");
  }
}
