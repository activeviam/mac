/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.descriptions;

import com.activeviam.pivot.builders.StartBuilding;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.literal.ILiteralType;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.impl.ActivePivotManagerBuilder;

public class MicroApplication {

  public static final int MICROAPP_CHUNK_SIZE = 256;

  private MicroApplication() {
  }

  public static IDatastoreSchemaDescription datastoreDescription() {
    return StartBuilding.datastoreSchema()
        .withStore(
            StartBuilding.store()
                .withStoreName("A")
                .withField("id", ILiteralType.INT)
                .asKeyField()
                .withChunkSize(MICROAPP_CHUNK_SIZE)
                .build())
        .build();
  }

  public static IActivePivotManagerDescription managerDescription(
      final IDatastoreSchemaDescription schemaDescription) {
    final IActivePivotManagerDescription managerDescription = StartBuilding.managerDescription()
        .withSchema()
        .withSelection(
            StartBuilding.selection(schemaDescription)
                .fromBaseStore("A")
                .withAllFields()
                .build())
        .withCube(
            StartBuilding.cube("Cube")
                .withContributorsCount()
                .withSingleLevelDimension("id")
                .asDefaultHierarchy()
                .build())
        .build();

    return ActivePivotManagerBuilder.postProcess(managerDescription, schemaDescription);
  }
}
