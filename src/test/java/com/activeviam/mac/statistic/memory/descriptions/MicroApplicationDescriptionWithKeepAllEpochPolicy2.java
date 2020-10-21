/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.descriptions;

import com.qfs.condition.impl.BaseConditions;
import com.qfs.store.IDatastore;
import com.qfs.store.transaction.DatastoreTransactionException;
import java.util.stream.IntStream;

// todo refactor fill method
public class MicroApplicationDescriptionWithKeepAllEpochPolicy2
		extends MicroApplicationDescriptionWithKeepAllEpochPolicy {

	@Override
	public void fill(IDatastore datastore) throws DatastoreTransactionException {
		// epoch 1
		datastore.edit(transactionManager -> {
			IntStream.range(0, 10)
					.forEach(i -> transactionManager.add("A", i, 0.));
		});

		// epoch 2
		datastore.edit(transactionManager -> {
			IntStream.range(10, 20)
					.forEach(i -> transactionManager.add("A", i, 1.));
		});

		// epoch 3
		// drop partition from epoch 2
		datastore.edit(transactionManager -> {
			transactionManager.removeWhere("A", BaseConditions.Equal("value", 1.));
		});

		// epoch 4
		// make sure to add a new chunk on the 0-valued partition
		datastore.edit(transactionManager -> {
			IntStream.range(20, 20 + CHUNK_SIZE)
					.forEach(i -> transactionManager.add("A", i, 0.));
		});

		// epoch 5
		// remaining chunks from epoch 4, but not used by version
		datastore.edit(transactionManager -> {
			transactionManager.removeWhere("A", BaseConditions.GreaterOrEqual("id", 20));
		});
	}
}
