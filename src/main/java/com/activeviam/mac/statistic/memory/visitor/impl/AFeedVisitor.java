/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.statistic.memory.visitor.impl;

import com.activeviam.mac.memory.DatastoreConstants;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.visitor.IMemoryStatisticVisitor;
import com.qfs.store.IDatastoreSchemaMetadata;
import com.qfs.store.record.IRecordFormat;
import com.qfs.store.transaction.IOpenedTransaction;

public abstract class AFeedVisitor<R> implements IMemoryStatisticVisitor<R> {

	protected final IOpenedTransaction transaction;
	protected final IDatastoreSchemaMetadata storageMetadata;
	protected final String dumpName;

	public AFeedVisitor(
			final IOpenedTransaction transaction,
			final IDatastoreSchemaMetadata storageMetadata,
			final String dumpName) {
		this.transaction = transaction;
		this.storageMetadata = storageMetadata;
		this.dumpName = dumpName;
	}

	/**
	 * Visits all the children of the given {@link IMemoryStatistic}.
	 *
	 * @param statistic The statistics whose children to visit.
	 */
	protected void visitChildren(final IMemoryStatistic statistic) {
		for (final IMemoryStatistic child : statistic.getChildren()) {
			child.accept(this);
		}
	}

	protected static IRecordFormat getChunkFormat(IDatastoreSchemaMetadata storageMetadata) {
		return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.CHUNK_STORE);
	}

	protected static IRecordFormat getProviderFormat(IDatastoreSchemaMetadata storageMetadata) {
		return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.PROVIDER_STORE);
	}

	protected static IRecordFormat getLevelFormat(IDatastoreSchemaMetadata storageMetadata) {
		return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.LEVEL_STORE);
	}

	private static IRecordFormat getIndexFormat(IDatastoreSchemaMetadata storageMetadata) {
		return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.INDEX_STORE);
	}

	private static IRecordFormat getReferenceFormat(IDatastoreSchemaMetadata storageMetadata) {
		return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.REFERENCE_STORE);
	}


}
