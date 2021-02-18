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
import com.qfs.store.IDatastore;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.impl.ActivePivotManagerDescription;
import java.util.stream.IntStream;

public class MicroApplicationDescriptionWithSharedVectorField
    implements ITestApplicationDescription {

  public static final int VECTOR_BLOCK_SIZE = 64;
  public static final int CHUNK_SIZE = 256;
  public static final int FIELD_SHARING_COUNT = 2;
  public static final int ADDED_DATA_SIZE = 20;

  @Override
  public IDatastoreSchemaDescription datastoreDescription() {
    return StartBuilding.datastoreSchema()
        .withStore(
            StartBuilding.store()
                .withStoreName("A")
                .withField("id", ILiteralType.INT)
                .asKeyField()
                .withVectorField("vector1", ILiteralType.DOUBLE)
                .withVectorBlockSize(VECTOR_BLOCK_SIZE)
                .withVectorField("vector2", ILiteralType.DOUBLE)
                .withVectorBlockSize(VECTOR_BLOCK_SIZE)
                .withChunkSize(CHUNK_SIZE)
                .build())
        .build();
  }

  @Override
  public IActivePivotManagerDescription managerDescription(
      IDatastoreSchemaDescription schemaDescription) {
    return new ActivePivotManagerDescription();
  }

  public static void fillWithGenericData(IDatastore datastore, IActivePivotManager manager) {
    datastore.edit(
        tm ->
            IntStream.range(0, ADDED_DATA_SIZE)
                .forEach(i -> tm.add("A", i * i, new double[] {i}, new double[] {-i, -i * i})));
  }
}
