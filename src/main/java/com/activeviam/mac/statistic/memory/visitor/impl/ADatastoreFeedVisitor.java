/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription.ParentType;
import com.qfs.store.IDatastoreSchemaMetadata;
import com.qfs.store.record.IRecordFormat;
import com.qfs.store.transaction.IOpenedTransaction;

/**
 * @author ActiveViam
 * @param <R>
 */
public abstract class ADatastoreFeedVisitor<R> extends AFeedVisitor<R> {

  /** The name of the store of the visited statistic */
  protected String store = null;
  /** Name of the currently visited field */
  protected String field = null;
  /** ID of the current index */
  protected Long indexId;
  /** ID of the current reference. */
  protected Long referenceId;

  /** ID of the current dictionary. */
  protected Long dictionaryId;

  /**
   * Constructor
   *
   * @param transaction Ongoing loading transaction
   * @param storageMetadata Schema metadata
   * @param dumpName dumpName of the ongoing import
   */
  public ADatastoreFeedVisitor(
      IOpenedTransaction transaction, IDatastoreSchemaMetadata storageMetadata, String dumpName) {
    super(transaction, storageMetadata, dumpName);
  }

  /**
   * Adds field parent data to the join store
   *
   * @param type {@code ParentType} of the owner of the field-related chunk
   * @param id id of the owner of the field-related chunk
   */
  protected void recordFieldForStructure(final ParentType type, final String id) {
    if (this.store != null && this.field != null) {
      final IRecordFormat format =
          FeedVisitor.getRecordFormat(
              this.storageMetadata, DatastoreConstants.CHUNK_TO_FIELD_STORE);
      final Object[] tuple = new Object[format.getFieldCount()];
      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);
      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.CHUNK_TO_FIELD__STORE, this.store);
      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.CHUNK_TO_FIELD__FIELD, this.field);
      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.CHUNK_TO_FIELD__PARENT_TYPE, type);
      FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK_TO_FIELD__PARENT_ID, id);

      this.transaction.add(DatastoreConstants.CHUNK_TO_FIELD_STORE, tuple);
    }
  }

  /**
   * Adds index parent data to the join store
   *
   * @param type {@code ParentType} of the owner of the index-related chunk
   * @param id id of the owner of the index-related chunk
   */
  protected void recordIndexForStructure(final ParentType type, final String id) {
    if (this.indexId != null) {
      final IRecordFormat format =
          FeedVisitor.getRecordFormat(
              this.storageMetadata, DatastoreConstants.CHUNK_TO_INDEX_STORE);
      final Object[] tuple = new Object[format.getFieldCount()];
      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);
      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.CHUNK_TO_INDEX__INDEX_ID, this.indexId);
      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.CHUNK_TO_INDEX__PARENT_TYPE, type);
      FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK_TO_INDEX__PARENT_ID, id);

      this.transaction.add(DatastoreConstants.CHUNK_TO_INDEX_STORE, tuple);
    }
  }

  /**
   * Adds reference parent data to the join store
   *
   * @param type {@code ParentType} of the owner of the reference-related chunk
   * @param id id of the owner of the reference-related chunk
   */
  protected void recordRefForStructure(final ParentType type, final String id) {
    if (this.referenceId != null) {
      final IRecordFormat format =
          FeedVisitor.getRecordFormat(this.storageMetadata, DatastoreConstants.CHUNK_TO_REF_STORE);
      final Object[] tuple = new Object[format.getFieldCount()];
      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);
      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.CHUNK_TO_REF__REF_ID, this.referenceId);
      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.CHUNK_TO_REF__PARENT_TYPE, type);
      FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK_TO_REF__PARENT_ID, id);

      this.transaction.add(DatastoreConstants.CHUNK_TO_REF_STORE, tuple);
    }
  }

  /**
   * Adds all underlying parent data to the respective join stores
   *
   * @param type type {@code ParentType} of the owner of the chunk
   * @param id id of the owner of the chunk
   */
  protected void recordStructureParent(final ParentType type, final String id) {
    recordFieldForStructure(type, id);
    recordIndexForStructure(type, id);
    recordRefForStructure(type, id);
    // recordDicoForStructure(type, id);
  }
}
