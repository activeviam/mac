/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.descriptions;

import com.activeviam.pivot.builders.StartBuilding;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.impl.ActivePivotManagerBuilder;

public class FullApplicationDescriptionWithAggregatedVectors
		extends FullApplicationDescriptionWithVectors {

	@Override
	public IActivePivotManagerDescription managerDescription(
			IDatastoreSchemaDescription schemaDescription) {
		final IActivePivotManagerDescription managerDescription = StartBuilding.managerDescription()
				.withSchema()
				.withSelection(
						StartBuilding.selection(schemaDescription)
								.fromBaseStore(VECTOR_STORE_NAME)
								.withAllFields()
								.build())
				.withCube(
						StartBuilding.cube("C")
								.withContributorsCount()
								.withAggregatedMeasure()
								.sum("vectorInt1")
								.withAggregatedMeasure()
								.avg("vectorLong")
								.withSingleLevelDimension("Id")
								.withPropertyName("vectorId")
								.withAggregateProvider()
								.leaf()
								.build())
				.build();

		return ActivePivotManagerBuilder.postProcess(managerDescription, schemaDescription);
	}
}
