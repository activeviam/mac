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

/**
 * Abstract class for {@link IMemoryStatisticVisitor memory statistic visitors}.
 *
 * @param <R> Type of the result to return by the visitor
 * @author ActiveViam
 */
public abstract class AFeedVisitor<R> implements IMemoryStatisticVisitor<R> {

	/**
	 * Ongoing transaction.
	 */
	protected final IOpenedTransaction transaction;
	/**
	 * Metadata of the Analysis Datastore.
	 */
	protected final IDatastoreSchemaMetadata storageMetadata;
	/**
	 * Name of the import.
	 */
	protected final String dumpName;

	/**
	 * Constuctor.
	 *
	 * @param transaction     transaction to fill with visited statistics
	 * @param storageMetadata Metadata of the Analysis Datastore
	 * @param dumpName        Name of the import
	 */
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

	/**
	 * Returns the {@link IRecordFormat } of the Chunk store.
	 *
	 * @param storageMetadata metadata of the application datastore
	 * @return the {@link DatastoreConstants#CHUNK_STORE} record format
	 */
	protected static IRecordFormat getChunkFormat(IDatastoreSchemaMetadata storageMetadata) {
		return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.CHUNK_STORE);
	}

	/**
	 * Returns the {@link IRecordFormat } of the Provider component store.
	 *
	 * @return the {@link DatastoreConstants#PROVIDER_COMPONENT_STORE} record format
	 */
	protected IRecordFormat getProviderCpnFormat() {
		return FeedVisitor.getRecordFormat(
				this.storageMetadata, DatastoreConstants.PROVIDER_COMPONENT_STORE);
	}

	/**
	 * Returns the {@link IRecordFormat } of the Provider store.
	 *
	 * @param storageMetadata metadata of the application datastore
	 * @return the {@link DatastoreConstants#PROVIDER_STORE} record format
	 */
	protected static IRecordFormat getProviderFormat(IDatastoreSchemaMetadata storageMetadata) {
		return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.PROVIDER_STORE);
	}

	/**
	 * Returns the {@link IRecordFormat } of the Level store.
	 *
	 * @param storageMetadata metadata of the application datastore
	 * @return the {@link DatastoreConstants#LEVEL_STORE} record format
	 */
	protected static IRecordFormat getLevelFormat(IDatastoreSchemaMetadata storageMetadata) {
		return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.LEVEL_STORE);
	}

	/**
	 * Returns the {@link IRecordFormat } of the Index store.
	 *
	 * @param storageMetadata metadata of the application datastore
	 * @return the {@link DatastoreConstants#INDEX_STORE} record format
	 */
	protected static IRecordFormat getIndexFormat(IDatastoreSchemaMetadata storageMetadata) {
		return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.INDEX_STORE);
	}

	/**
	 * Returns the {@link IRecordFormat } of the Reference store.
	 *
	 * @param storageMetadata metadata of the application datastore
	 * @return the {@link DatastoreConstants#REFERENCE_STORE} record format
	 */
	protected static IRecordFormat getReferenceFormat(IDatastoreSchemaMetadata storageMetadata) {
		return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.REFERENCE_STORE);
	}

	/**
	 * Returns the {@link IRecordFormat } of the Dictionary store.
	 *
	 * @param storageMetadata metadata of the application datastore
	 * @return the {@link DatastoreConstants#DICTIONARY_STORE} record format
	 */
	protected static IRecordFormat getDictionaryFormat(IDatastoreSchemaMetadata storageMetadata) {
		return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.DICTIONARY_STORE);
	}
}
