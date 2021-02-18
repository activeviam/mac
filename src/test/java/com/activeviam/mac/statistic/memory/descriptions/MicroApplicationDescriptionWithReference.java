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

public class MicroApplicationDescriptionWithReference implements ITestApplicationDescription {
  public static final int CHUNK_SIZE = 256;

  @Override
  public IDatastoreSchemaDescription datastoreDescription() {
    return StartBuilding.datastoreSchema()
        .withStore(
            StartBuilding.store()
                .withStoreName("A")
                .withField("id", ILiteralType.INT)
                .asKeyField()
                .withField("val", ILiteralType.INT)
                .withChunkSize(CHUNK_SIZE)
                .build())
        .withStore(
            StartBuilding.store()
                .withStoreName("B")
                .withField("tgt_id", ILiteralType.INT)
                .asKeyField()
                .withChunkSize(CHUNK_SIZE)
                .build())
        .withReference(
            StartBuilding.reference()
                .fromStore("A")
                .toStore("B")
                .withName("AToB")
                .withMapping("val", "tgt_id")
                .build())
        .build();
  }

  @Override
  public IActivePivotManagerDescription managerDescription(
      IDatastoreSchemaDescription schemaDescription) {
    final IActivePivotManagerDescription managerDescription =
        StartBuilding.managerDescription()
            .withSchema()
            .withSelection(
                StartBuilding.selection(schemaDescription)
                    .fromBaseStore("A")
                    .withAllReachableFields()
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
