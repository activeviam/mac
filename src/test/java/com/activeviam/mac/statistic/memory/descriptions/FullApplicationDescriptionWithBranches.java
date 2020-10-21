/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.descriptions;

import com.qfs.store.IDatastore;
import com.qfs.store.NoTransactionException;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.qfs.store.transaction.ITransactionManager;
import java.time.LocalDate;
import java.util.Collection;
import java.util.Random;
import java.util.stream.IntStream;

public class FullApplicationDescriptionWithBranches extends FullApplicationDescription {

	private final Collection<String> branches;

	public FullApplicationDescriptionWithBranches(Collection<String> branches) {
		super();
		this.branches = branches;
	}

	@Override
	public void fill(IDatastore datastore) {
		super.fill(datastore);
		branches.forEach(
				br_string -> {
					ITransactionManager tm = datastore.getTransactionManager();
					try {
						tm.startTransactionOnBranch(br_string, "Sales");
					} catch (IllegalArgumentException | DatastoreTransactionException e) {
						throw new RuntimeException(e);
					}
					final Random r = new Random(47605);
					IntStream.range(operationsBatch.getAndIncrement(), 1000 * operationsBatch.get())
							.forEach(
									i -> {
										final int seller = r.nextInt(STORE_PEOPLE_COUNT);
										int buyer;
										do {
											buyer = r.nextInt(STORE_PEOPLE_COUNT);
										} while (buyer == seller);
										tm.add(
												"Sales",
												i,
												String.valueOf(seller),
												String.valueOf(buyer),
												LocalDate.now().plusDays(-r.nextInt(7)),
												(long) r.nextInt(STORE_PRODUCT_COUNT));
									});
					try {
						tm.commitTransaction();
					} catch (NoTransactionException | DatastoreTransactionException e) {
						throw new RuntimeException(e);
					}
				});
	}
}
