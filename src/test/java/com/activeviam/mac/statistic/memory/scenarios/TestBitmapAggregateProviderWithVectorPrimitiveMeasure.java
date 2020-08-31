/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.scenarios;

import com.activeviam.mac.statistic.memory.ASingleAppMonitoringTest;
import com.activeviam.pivot.builders.StartBuilding;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.literal.ILiteralType;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.store.IDatastore;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.impl.ActivePivotManagerBuilder;
import java.util.Collection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The scenario this test produces created a situation where the dictionary used by the point index
 * of the aggregate store of the cube would leak into subsequent chunks.
 */
public class TestBitmapAggregateProviderWithVectorPrimitiveMeasure extends
    ASingleAppMonitoringTest {

  protected static final int RECORD_COUNT = 100;

  @Override
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

  @Override
  protected IActivePivotManagerDescription managerDescription(
      final IDatastoreSchemaDescription datastoreSchema) {
    final IActivePivotManagerDescription managerDescription = StartBuilding.managerDescription()
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

  @Override
  protected void beforeExport(
      IDatastore datastore, IActivePivotManager manager) {
    datastore.edit(transactionManager -> {
      for (int i = 0; i < RECORD_COUNT; ++i) {
        transactionManager.add("Store", i, new double[] {i, -i});
      }
    });
  }

  @Override
  protected void feedStatisticsIntoDatastore(
      final Collection<? extends IMemoryStatistic> statistics, final IDatastore analysisDatastore) {
    // do nothing here so we can do the loading inside the test instead
  }

  @Test
  public void testLoading() {
    Assertions
        .assertDoesNotThrow(
            () -> super.feedStatisticsIntoDatastore(statistics, monitoringDatastore));
  }
}
