/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

import com.qfs.store.IDatastoreSchemaMetadata;
import com.qfs.store.transaction.IOpenedTransaction;

/**
 * Visitor for chunks from a store export.
 *
 * @param <R> Type of the result to return by the visitor
 * @author ActiveViam
 */
public abstract class ADatastoreFeedVisitor<R> extends AFeedVisitor<R> {

  /** The name of the store of the visited statistic. */
  protected String store = null;
  /** Name of the currently visited field. */
  protected String field = null;
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
}
