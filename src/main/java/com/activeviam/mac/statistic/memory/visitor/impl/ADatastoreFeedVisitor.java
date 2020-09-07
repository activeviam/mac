/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

import com.qfs.store.IDatastoreSchemaMetadata;
import com.qfs.store.transaction.IOpenedTransaction;
import java.util.Collection;

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
  protected Collection<String> fields;
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

  /**
   * Retrieves the single field name from {@link #fields}.
   *
   * <p>This asserts that there is only one element in the {@link #fields} collection.
   *
   * @return the field
   */
  protected String retrieveUniqueField() {
    assert (this.fields.size() == 1);
    return this.fields.iterator().next();
  }
}
