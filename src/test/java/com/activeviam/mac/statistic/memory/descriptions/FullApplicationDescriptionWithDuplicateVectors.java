/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.descriptions;

import com.qfs.store.IDatastore;
import com.qfs.store.query.IDictionaryCursor;

public class FullApplicationDescriptionWithDuplicateVectors
		extends FullApplicationDescriptionWithVectors {

	@Override
	public void fill(IDatastore datastore) {
		final int vectorSize = 10;

		// 3 vectors of same size with same values (but not copied one from another), v1, v3 of ints and
		// v2 of long
		final int[] v1 = new int[vectorSize];
		final int[] v3 = new int[vectorSize];
		final long[] v2 = new long[vectorSize];
		for (int j = 0; j < vectorSize; j++) {
			v1[j] = j;
			v2[j] = j;
			v3[j] = j;
		}

		IDictionaryCursor cursor =
				datastore
						.getHead()
						.getQueryManager()
						.forStore(VECTOR_STORE_NAME)
						.withoutCondition()
						.selecting("vectorInt1")
						.run();

		final Object vec = cursor.next() ? cursor.getRecord().read("vectorInt1") : null;

		datastore.edit(
				tm -> {
					tm.add(VECTOR_STORE_NAME, 0, v1, vec, v2);
				});
	}
}
