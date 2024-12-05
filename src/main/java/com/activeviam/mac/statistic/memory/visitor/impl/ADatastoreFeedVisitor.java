/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

import com.activeviam.database.api.schema.IDatabaseSchema;
import com.activeviam.database.datastore.api.transaction.IOpenedTransaction;
import java.util.Collection;

/**
 * Visitor for chunks from a store export.
 *
 * @param <R> Type of the result to return by the visitor
 * @author ActiveViam
 */
public abstract class ADatastoreFeedVisitor<R> extends AFeedVisitorWithDictionary<R> {

  /** Names of the currently visited fields. */
  protected Collection<String> fields;

  /** ID of the current index. */
  protected Long indexId;

  /** ID of the current reference. */
  protected Long referenceId;

  /**
   * Constructor.
   *
   * @param transaction Ongoing loading transaction
   * @param storageMetadata Schema metadata
   * @param dumpName dumpName of the ongoing import
   */
  public ADatastoreFeedVisitor(
      IOpenedTransaction transaction, IDatabaseSchema storageMetadata, String dumpName) {
    super(transaction, storageMetadata, dumpName);
  }
}
