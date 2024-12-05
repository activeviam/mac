/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

import com.activeviam.database.api.schema.IDataTable;
import com.activeviam.database.api.schema.IDatabaseSchema;
import com.activeviam.database.datastore.api.transaction.IOpenedTransaction;
import com.activeviam.mac.entities.ChunkOwner;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.tech.observability.internal.memory.AMemoryStatistic;
import com.activeviam.tech.observability.internal.memory.IMemoryStatisticVisitor;
import com.activeviam.tech.records.api.IRecordFormat;

/**
 * Abstract class for {@link IMemoryStatisticVisitor memory statistic visitors}.
 *
 * @param <R> Type of the result to return by the visitor
 * @author ActiveViam
 */
public abstract class AFeedVisitor<R> implements IMemoryStatisticVisitor<R> {

  /** Ongoing transaction. */
  protected final IOpenedTransaction transaction;

  /** Metadata of the Analysis Datastore. */
  protected final IDatabaseSchema storageMetadata;

  /** Name of the import. */
  protected final String dumpName;

  /** Owner of the visited statistics. */
  protected ChunkOwner owner = null;

  /**
   * Constructor.
   *
   * @param transaction transaction to fill with visited statistics
   * @param storageMetadata Metadata of the Analysis Datastore
   * @param dumpName Name of the import
   */
  public AFeedVisitor(
      final IOpenedTransaction transaction,
      final IDatabaseSchema storageMetadata,
      final String dumpName) {
    this.transaction = transaction;
    this.storageMetadata = storageMetadata;
    this.dumpName = dumpName;
  }

  /**
   * Returns the {@link IRecordFormat } of the Chunk store.
   *
   * @param storageMetadata metadata of the application datastore
   * @return the {@link DatastoreConstants#CHUNK_STORE} record format
   */
  protected static IDataTable getChunkFormat(IDatabaseSchema storageMetadata) {
    return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.CHUNK_STORE);
  }

  /**
   * Returns the {@link IRecordFormat} of the Provider store.
   *
   * @param storageMetadata metadata of the application datastore
   * @return the {@link DatastoreConstants#PROVIDER_STORE} record format
   */
  protected static IDataTable getProviderFormat(IDatabaseSchema storageMetadata) {
    return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.PROVIDER_STORE);
  }

  /**
   * Returns the {@link IRecordFormat} of the Level store.
   *
   * @param storageMetadata metadata of the application datastore
   * @return the {@link DatastoreConstants#LEVEL_STORE} record format
   */
  protected static IDataTable getLevelFormat(IDatabaseSchema storageMetadata) {
    return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.LEVEL_STORE);
  }

  /**
   * Returns the {@link IRecordFormat} of the Index store.
   *
   * @param storageMetadata metadata of the application datastore
   * @return the {@link DatastoreConstants#INDEX_STORE} record format
   */
  protected static IDataTable getIndexFormat(IDatabaseSchema storageMetadata) {
    return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.INDEX_STORE);
  }

  /**
   * Returns the {@link IRecordFormat} of the Reference store.
   *
   * @param storageMetadata metadata of the application datastore
   * @return the {@link DatastoreConstants#REFERENCE_STORE} record format
   */
  protected static IDataTable getReferenceFormat(IDatabaseSchema storageMetadata) {
    return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.REFERENCE_STORE);
  }

  /**
   * Returns the {@link IRecordFormat} of the Branch store.
   *
   * @param storageMetadata metadata of the application datastore
   * @return the {@link DatastoreConstants#VERSION_STORE} record format
   */
  protected static IDataTable getVersionStoreFormat(IDatabaseSchema storageMetadata) {
    return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.VERSION_STORE);
  }

  /**
   * Returns the {@link IRecordFormat} of the Dictionary store.
   *
   * @param storageMetadata metadata of the application datastore
   * @return the {@link DatastoreConstants#DICTIONARY_STORE} record format
   */
  protected static IDataTable getDictionaryFormat(IDatabaseSchema storageMetadata) {
    return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.DICTIONARY_STORE);
  }

  /**
   * Visits all the children of the given {@link AMemoryStatistic}.
   *
   * @param statistic The statistics whose children to visit.
   */
  protected void visitChildren(final AMemoryStatistic statistic) {
    for (final AMemoryStatistic child : statistic.getChildren()) {
      child.accept(this);
    }
  }
}
