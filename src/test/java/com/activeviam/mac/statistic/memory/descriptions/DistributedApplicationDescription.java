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
import com.qfs.messenger.impl.LocalMessenger;
import com.qfs.multiversion.IEpochManagementPolicy;
import com.qfs.multiversion.impl.KeepAllEpochPolicy;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;

public class DistributedApplicationDescription implements ITestApplicationDescription {

  public static final int CHUNK_SIZE = 256;

  @Override
  public IDatastoreSchemaDescription datastoreDescription() {
    return  StartBuilding.datastoreSchema()
        .withStore(
            StartBuilding.store()
                .withStoreName("A")
                .withField("id", ILiteralType.INT)
                .asKeyField()
                .withField("value", ILiteralType.DOUBLE)
                .asKeyField()
                .withChunkSize(CHUNK_SIZE)
                .withValuePartitioningOn("value")
                .build())
        .build();
  }

  @Override
  public IActivePivotManagerDescription managerDescription(
      IDatastoreSchemaDescription schemaDescription) {
    return StartBuilding.managerDescription()
        .withSchema()
        .withSelection(
            StartBuilding.selection(schemaDescription)
                .fromBaseStore("A")
                .withAllFields()
                .build())
        .withCube(
            StartBuilding.cube()
                .withName("Data")
                .withAggregatedMeasure()
                .sum("value")
                .withSingleLevelDimension("id")
                .build())
        .withDistributedCube(
            StartBuilding.cube("QueryCubeA")
                .withContributorsCount()
                .asQueryCube()
                .withClusterDefinition()
                .withClusterId("cluster")
                .withMessengerDefinition()
                .withKey(LocalMessenger.PLUGIN_KEY)
                .withNoProperty()
                .end()
                .withApplication("app")
                .withDistributingFields("value")
                .end()
                .withEpochDimension()
                .build())
        .withDistributedCube(
            StartBuilding.cube("QueryCubeB")
                .asQueryCube()
                .withClusterDefinition()
                .withClusterId("cluster")
                .withMessengerDefinition()
                .withKey(LocalMessenger.PLUGIN_KEY)
                .withNoProperty()
                .end()
                .withApplication("app")
                .withoutDistributingFields()
                .end()
                .withEpochDimension()
                .build())
        .build();
  }

  @Override
  public IEpochManagementPolicy epochManagementPolicy() {
    return new KeepAllEpochPolicy();
  }
}
