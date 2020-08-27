/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

import com.activeviam.mac.memory.DatastoreConstants;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.store.IDatastoreSchemaMetadata;
import com.qfs.store.record.IRecordFormat;
import com.qfs.store.transaction.IOpenedTransaction;
import java.util.Collection;
import java.util.Stack;

/**
 * Visitor for chunks from a store export.
 *
 * @param <R> Type of the result to return by the visitor
 * @author ActiveViam
 */
public abstract class ADatastoreFeedVisitor<R> extends AFeedVisitor<R> {

  /** The name of the store of the visited statistic. */
  protected String store = null;
  /** Names of the currently visited fields. */
  protected Stack<Collection<String>> fields = new Stack<>();
  /** ID of the current index. */
  protected Long indexId;
  /** ID of the current reference. */
  protected Long referenceId;

  /** ID of the current dictionary. */
  protected Long dictionaryId;

  /**
   * Constructor.
   *
   * @param transaction Ongoing loading transaction
   * @param storageMetadata Schema metadata
   * @param dumpName dumpName of the ongoing import
   */
  public ADatastoreFeedVisitor(
      IOpenedTransaction transaction, IDatastoreSchemaMetadata storageMetadata, String dumpName) {
    super(transaction, storageMetadata, dumpName);
  }

  protected void writeFieldRecordsForChunk(final IMemoryStatistic statistic) {
    final IRecordFormat format = getFieldFormat(this.storageMetadata);
    Object[] tuple = FeedVisitor.buildFieldTupleFrom(format, statistic);

    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);

    this.fields.stream().flatMap(Collection::stream)
        .forEachOrdered(field -> {
          FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.FIELD__FIELD_NAME, field);
          FeedVisitor.add(statistic, this.transaction, DatastoreConstants.FIELD_STORE, tuple);
        });
  }
}
