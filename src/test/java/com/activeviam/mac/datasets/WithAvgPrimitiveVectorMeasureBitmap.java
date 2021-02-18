/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.datasets;

import com.activeviam.pivot.builders.StartBuilding;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.literal.ILiteralType;
import com.qfs.pivot.monitoring.impl.MemoryAnalysisService;
import com.qfs.service.monitoring.IMemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.impl.ActivePivotDatastorePostProcessor;
import com.quartetfs.biz.pivot.impl.ActivePivotManagerBuilder;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.contributions.impl.ClasspathContributionProvider;
import java.nio.file.Path;

public class WithAvgPrimitiveVectorMeasureBitmap {

  private static final Path EXPORT_PATH = Path.of("sampleStats");

  public static void main(String[] args) throws AgentException {
    Registry.setContributionProvider(new ClasspathContributionProvider());

    final IDatastoreSchemaDescription datastoreSchemaDescription =
        StartBuilding.datastoreSchema()
            .withStore(
                StartBuilding.store()
                    .withStoreName("a")
                    .withField("id", ILiteralType.INT)
                    .asKeyField()
                    .withField("hierarchy")
                    .withVectorField("measure", ILiteralType.DOUBLE)
                    .build())
            .build();

    IActivePivotManagerDescription managerDescription =
        StartBuilding.managerDescription()
            .withName("manager")
            .withSchema()
            .withSelection(
                StartBuilding.selection(datastoreSchemaDescription)
                    .fromBaseStore("a")
                    .withAllReachableFields()
                    .build())
            .withCube(
                StartBuilding.cube("cube")
                    .withContributorsCount()
                    .withAggregatedMeasure()
                    .sum("measure")
                    .withName("measure.SUM")
                    .withAggregatedMeasure()
                    .avg("measure")
                    .withName("measure.AVG")
                    .withSingleLevelDimension("id")
                    .withPropertyName("id")
                    .withSingleLevelDimension("hierarchy")
                    .withPropertyName("hierarchy")
                    .withAggregateProvider()
                    .bitmap()
                    .build())
            .build();

    managerDescription =
        ActivePivotManagerBuilder.postProcess(managerDescription, datastoreSchemaDescription);

    final IDatastore datastore =
        StartBuilding.datastore()
            .setSchemaDescription(datastoreSchemaDescription)
            .addSchemaDescriptionPostProcessors(
                ActivePivotDatastorePostProcessor.createFrom(managerDescription))
            .build();

    datastore.edit(
        tm -> {
          tm.add("a", 0, "member", new double[] {1.});
          tm.add("a", 1, "member", new double[] {2.});
          tm.add("a", 2, "member", new double[] {3.});
        });

    final IActivePivotManager manager =
        StartBuilding.manager()
            .setDatastoreAndPermissions(datastore)
            .setDescription(managerDescription)
            .buildAndStart();

    final IMemoryAnalysisService analysisService =
        new MemoryAnalysisService(datastore, manager, datastore.getEpochManager(), EXPORT_PATH);
    analysisService.exportMostRecentVersion(WithAvgPrimitiveVectorMeasureBitmap.class.getName());

    manager.stop();
    datastore.close();
  }
}
