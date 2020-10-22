/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.scenarios;

import com.activeviam.mac.statistic.memory.Application;
import com.activeviam.mac.statistic.memory.MonitoringTestUtils;
import com.activeviam.mac.statistic.memory.TestMACMeasures;
import com.activeviam.mac.statistic.memory.descriptions.ITestApplicationDescription;
import com.activeviam.mac.statistic.memory.junit.RegistrySetupExtension;
import com.activeviam.pivot.builders.StartBuilding;
import com.activeviam.properties.impl.ActiveViamProperty;
import com.activeviam.properties.impl.ActiveViamPropertyExtension;
import com.activeviam.properties.impl.ActiveViamPropertyExtension.ActiveViamPropertyExtensionBuilder;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.junit.LocalResourcesExtension;
import com.qfs.literal.ILiteralType;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.store.IDatastore;
import com.qfs.util.impl.QfsFileTestUtils;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.impl.ActivePivotManagerBuilder;
import com.quartetfs.fwk.AgentException;
import java.nio.file.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * The scenario this test produces created a situation where the dictionary used by the point index
 * of the aggregate store of the cube would leak into subsequent chunks.
 */
@ExtendWith(RegistrySetupExtension.class)
public class TestBitmapAggregateProviderWithVectorPrimitiveMeasure {

  public static final int RECORD_COUNT = 10;

  @RegisterExtension
  protected static ActiveViamPropertyExtension propertyExtension =
      new ActiveViamPropertyExtensionBuilder()
          .withProperty(ActiveViamProperty.ACTIVEVIAM_TEST_PROPERTY, true)
          .build();

  @RegisterExtension
  protected final LocalResourcesExtension resources = new LocalResourcesExtension();

  protected static Path tempDir = QfsFileTestUtils.createTempDirectory(TestMACMeasures.class);

  protected Application monitoredApplication;
  protected Path exportPath;

  @BeforeEach
  public void setup() throws AgentException {
    monitoredApplication = MonitoringTestUtils.setupApplication(
        new ScenarioApplication(),
        resources,
        ScenarioApplication::fill);

    exportPath = MonitoringTestUtils.exportMostRecentVersion(
        monitoredApplication.getDatastore(),
        monitoredApplication.getManager(),
        tempDir,
        this.getClass().getSimpleName());

  }

  @Test
  public void testLoading() {
    final IMemoryStatistic stats = MonitoringTestUtils.loadMemoryStatFromFolder(exportPath);
    Assertions.assertDoesNotThrow(() ->
        MonitoringTestUtils.createAnalysisDatastore(stats, resources));
  }

  private static final class ScenarioApplication implements ITestApplicationDescription {

    @Override
    public IDatastoreSchemaDescription datastoreDescription() {
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

    @Override
    public IActivePivotManagerDescription managerDescription(
        IDatastoreSchemaDescription schemaDescription) {
      final IActivePivotManagerDescription managerDescription = StartBuilding.managerDescription()
          .withSchema()
          .withSelection(
              StartBuilding.selection(schemaDescription)
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

      return ActivePivotManagerBuilder.postProcess(managerDescription, schemaDescription);
    }

    public static void fill(IDatastore datastore, IActivePivotManager manager) {
      datastore.edit(transactionManager -> {
        for (int i = 0; i < RECORD_COUNT; ++i) {
          transactionManager.add("Store", i, new double[] {i, -i});
        }
      });
    }
  }
}
