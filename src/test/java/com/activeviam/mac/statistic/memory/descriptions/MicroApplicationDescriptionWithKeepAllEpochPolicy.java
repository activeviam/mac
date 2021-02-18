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
import com.qfs.multiversion.IEpochManagementPolicy;
import com.qfs.multiversion.impl.KeepAllEpochPolicy;

public class MicroApplicationDescriptionWithKeepAllEpochPolicy extends MicroApplicationDescription {

  @Override
  public IDatastoreSchemaDescription datastoreDescription() {
    return StartBuilding.datastoreSchema()
        .withStore(
            StartBuilding.store()
                .withStoreName("A")
                .withField("id", ILiteralType.INT)
                .asKeyField()
                .withField("value", ILiteralType.DOUBLE)
                .asKeyField()
                .withValuePartitioningOn("value")
                .withChunkSize(CHUNK_SIZE)
                .build())
        .build();
  }

  @Override
  public IEpochManagementPolicy epochManagementPolicy() {
    return new KeepAllEpochPolicy();
  }
}
