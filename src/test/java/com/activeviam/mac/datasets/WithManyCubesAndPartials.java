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

import com.activeviam.desc.build.IBuildableAggregateProviderDescriptionBuilder;
import com.activeviam.desc.build.ICanStartPartialProvider;
import com.activeviam.pivot.builders.StartBuilding;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.literal.ILiteralType;
import com.qfs.pivot.monitoring.impl.MemoryAnalysisService;
import com.qfs.service.monitoring.IMemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.definitions.IActivePivotInstanceDescription;
import com.quartetfs.biz.pivot.definitions.impl.ActivePivotDatastorePostProcessor;
import com.quartetfs.biz.pivot.impl.ActivePivotManagerBuilder;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.contributions.impl.ClasspathContributionProvider;
import java.nio.file.Path;

public class WithManyCubesAndPartials {

  private static final Path EXPORT_PATH = Path.of("sampleStats");

  private static final int CUBE_COUNT = 20;
  private static final int PROVIDER_COUNT_PER_CUBE = 5;

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
                    .withField("measure", ILiteralType.DOUBLE)
                    .withVectorField("vectorMeasure", ILiteralType.DOUBLE)
                    .build())
            .build();

    var managerDescriptionBuilder =
        StartBuilding.managerDescription()
            .withName("manager")
            .withSchema()
            .withSelection(
                StartBuilding.selection(datastoreSchemaDescription)
                    .fromBaseStore("a")
                    .withAllReachableFields()
                    .build())
            .withCube(generateCubeDescription("cube 0", ProviderType.BITMAP));

    for (int i = 1; i < CUBE_COUNT; ++i) {
      managerDescriptionBuilder =
          managerDescriptionBuilder.withCube(
              generateCubeDescription("cube " + i, ProviderType.BITMAP));
    }

    final var managerDescription =
        ActivePivotManagerBuilder.postProcess(
            managerDescriptionBuilder.build(), datastoreSchemaDescription);

    final IDatastore datastore =
        StartBuilding.datastore()
            .setSchemaDescription(datastoreSchemaDescription)
            .addSchemaDescriptionPostProcessors(
                ActivePivotDatastorePostProcessor.createFrom(managerDescription))
            .build();

    datastore.edit(
        tm -> {
          tm.add("a", 0, "member", 1., new double[] {1., 2., 3.});
          tm.add("a", 1, "member", 2., new double[] {1., 2., 3.});
          tm.add("a", 2, "member", 3., new double[] {1., 2., 3.});
        });

    final IActivePivotManager manager =
        StartBuilding.manager()
            .setDatastoreAndPermissions(datastore)
            .setDescription(managerDescription)
            .buildAndStart();

    final IMemoryAnalysisService analysisService =
        new MemoryAnalysisService(datastore, manager, datastore.getEpochManager(), EXPORT_PATH);
    analysisService.exportMostRecentVersion(WithManyCubesAndPartials.class.getSimpleName());

    manager.stop();
    datastore.close();
  }

  protected enum ProviderType {
    LEAF,
    BITMAP,
    JIT
  }

  protected static IActivePivotInstanceDescription generateCubeDescription(
      String cubeName, ProviderType providerType) {
    final var cubeDesc =
        StartBuilding.cube(cubeName)
            .withContributorsCount()
            .withAggregatedMeasure()
            .sum("measure")
            .withName("measure.SUM")
            .withAggregatedMeasure()
            .avg("measure")
            .withName("measure.AVG")
            .withAggregatedMeasure()
            .max("measure")
            .withName("measure.MAX")
            .withAggregatedMeasure()
            .sum("vectorMeasure")
            .withName("vectorMeasure.SUM")
            .withSingleLevelDimension("id")
            .withPropertyName("id")
            .withSingleLevelDimension("hierarchy")
            .withPropertyName("hierarchy")
            .withAggregateProvider();

    IBuildableAggregateProviderDescriptionBuilder<IActivePivotInstanceDescription> withFull;
    switch (providerType) {
      case LEAF:
        withFull = cubeDesc.leaf();
        break;
      case BITMAP:
        withFull = cubeDesc.bitmap();
        break;
      case JIT:
        withFull = cubeDesc.jit();
        break;
      default:
        withFull = cubeDesc.jit();
        break;
    }

    for (int i = 0; i < PROVIDER_COUNT_PER_CUBE; ++i) {
      withFull = addPartial(withFull, ProviderType.BITMAP, "measure.SUM");
    }

    return withFull.build();
  }

  protected static IBuildableAggregateProviderDescriptionBuilder<IActivePivotInstanceDescription>
      addPartial(
          ICanStartPartialProvider<IActivePivotInstanceDescription> builder,
          ProviderType providerType,
          String... measures) {
    final var withPartial = builder.withPartialProvider();
    switch (providerType) {
      case LEAF:
        return withPartial.leaf().includingOnlyMeasures(measures);
      case BITMAP:
        return withPartial.bitmap().includingOnlyMeasures(measures);
      case JIT:
      default:
        return withPartial.bitmap().includingOnlyMeasures(measures);
    }
  }
}
