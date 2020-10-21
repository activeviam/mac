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
import com.qfs.store.IDatastore;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.qfs.store.transaction.ITransactionManager;
import java.util.stream.IntStream;

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
	public void fill(IDatastore datastore) throws DatastoreTransactionException {
		final ITransactionManager transactionManager = datastore.getTransactionManager();

		transactionManager.startTransactionOnBranch("branch1", "A");
		IntStream.range(0, 10).forEach(i ->
				transactionManager.add("A", i, 0.));
		transactionManager.commitTransaction();

		transactionManager.startTransactionOnBranch("branch2", "A");
		IntStream.range(10, 20).forEach(i ->
				transactionManager.add("A", i, 0.));
		transactionManager.commitTransaction();

		transactionManager.startTransactionFromBranch("subbranch", "branch2", "A");
		IntStream.range(20, 30).forEach(i ->
				transactionManager.add("A", i, 0.));
		transactionManager.commitTransaction();

		transactionManager.startTransactionOnBranch("branch1", "A");
		IntStream.range(10, 20).forEach(i ->
				transactionManager.add("A", i, 0.));
		transactionManager.commitTransaction();
	}

	@Override
	public IEpochManagementPolicy epochManagementPolicy() {
		return new KeepAllEpochPolicy();
	}
}
