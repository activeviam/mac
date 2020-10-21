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
import java.util.stream.IntStream;

public class MicroApplicationDescriptionWithReferenceAndSameFieldName
		extends MicroApplicationDescriptionWithReference {

	public static final int ADDED_DATA_SIZE = CHUNK_SIZE;

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
								.withField("val", ILiteralType.INT)
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
	public void fill(IDatastore datastore) {
		datastore.edit(tm -> {
			IntStream.range(0, ADDED_DATA_SIZE)
					.forEach(i -> tm.add("A", i, i * i));
			IntStream.range(0, 3 * ADDED_DATA_SIZE)
					.forEach(i -> tm.add("B", i, -i * i));
		});
	}
}
