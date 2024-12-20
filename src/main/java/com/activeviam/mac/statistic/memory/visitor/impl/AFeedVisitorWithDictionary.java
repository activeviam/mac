/*
 * (C) ActiveViam 2021
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

import static com.activeviam.tech.observability.internal.memory.MemoryStatisticConstants.ATTR_NAME_CREATOR_CLASS;
import static com.activeviam.tech.observability.internal.memory.MemoryStatisticConstants.ATTR_NAME_DICTIONARY_ID;

import com.activeviam.database.api.schema.IDatabaseSchema;
import com.activeviam.database.datastore.api.transaction.IOpenedTransaction;
import com.activeviam.database.datastore.private_.structure.impl.StructureDictionaryManager;
import com.activeviam.mac.Loggers;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.tech.observability.api.memory.IStatisticAttribute;
import com.activeviam.tech.observability.internal.memory.DictionaryStatistic;
import com.activeviam.tech.observability.internal.memory.MemoryStatisticConstants;
import java.util.logging.Logger;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public abstract class AFeedVisitorWithDictionary<R> extends AFeedVisitor<R> {

  /** The class logger. */
  private static final Logger LOGGER = Logger.getLogger(Loggers.LOADING);

  /** The attributes of the dictionary currently being visited. */
  protected DictionaryAttributes dictionaryAttributes = DictionaryAttributes.none();

  /**
   * Constructor.
   *
   * @param transaction transaction to fill with visited statistics
   * @param storageMetadata Metadata of the Analysis Datastore
   * @param dumpName Name of the import
   */
  public AFeedVisitorWithDictionary(
      IOpenedTransaction transaction, IDatabaseSchema storageMetadata, String dumpName) {
    super(transaction, storageMetadata, dumpName);
  }

  /**
   * Processes the given dictionary statistic, adding a record to the dictionary store if relevant.
   *
   * <p>Internal state ({@link #dictionaryAttributes}) is changed accordingly.
   *
   * @param statistic the statistic to process
   * @return the
   */
  protected DictionaryAttributes processDictionaryStatistic(
      DictionaryStatistic statistic, Long epochId) {
    final var previousAttributes = this.dictionaryAttributes;
    this.dictionaryAttributes = new DictionaryAttributes(previousAttributes);
    if (!statistic.getName().equals(MemoryStatisticConstants.STAT_NAME_DICTIONARY_UNDERLYING)) {
      final IStatisticAttribute dictionaryIdAttribute =
          statistic.getAttribute(ATTR_NAME_DICTIONARY_ID);
      if (dictionaryIdAttribute != null) {
        this.dictionaryAttributes.setDictionaryId(dictionaryIdAttribute.asLong());
      }

      final var classAttribute = statistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_CLASS);
      if (classAttribute != null) {
        this.dictionaryAttributes.setDictionaryClass(classAttribute.asText());
      } else if (previousAttributes.getDictionaryClass() == null) {
        LOGGER.warning(
            "Dictionary does not state its class."
                + " The following statistic assumes the creator's class as dictionary class : "
                + statistic);
        this.dictionaryAttributes.setDictionaryClass(
            statistic.getAttribute(ATTR_NAME_CREATOR_CLASS).asText());
      }

      final var sizeAttribute = statistic.getAttribute(DatastoreConstants.DICTIONARY_SIZE);
      if (sizeAttribute != null) {
        this.dictionaryAttributes.setDictionarySize(sizeAttribute.asInt());
      }

      final var orderAttribute = statistic.getAttribute(DatastoreConstants.DICTIONARY_ORDER);
      if (orderAttribute != null) {
        this.dictionaryAttributes.setDictionaryOrder(orderAttribute.asInt());
      }

      if (!this.dictionaryAttributes
          .getDictionaryClass()
          .equals(StructureDictionaryManager.class.getName())) {
        final var format = getDictionaryFormat(this.storageMetadata);
        final Object[] tuple =
            FeedVisitor.buildDictionaryTupleFrom(
                format,
                this.dictionaryAttributes.getDictionaryId(),
                this.dictionaryAttributes.getDictionaryClass(),
                this.dictionaryAttributes.getDictionarySize(),
                this.dictionaryAttributes.getDictionaryOrder());
        FeedVisitor.setTupleElement(
            tuple, format, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);
        FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.VERSION__EPOCH_ID, epochId);

        FeedVisitor.add(statistic, this.transaction, DatastoreConstants.DICTIONARY_STORE, tuple);
      }
    }
    return previousAttributes;
  }

  /** POJO class regrouping dictionary attributes. */
  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class DictionaryAttributes {

    /** An instance representing no dictionary (all attributes set to {@code null}. */
    public static final DictionaryAttributes NONE = new DictionaryAttributes();

    /** ID of the current dictionary. */
    protected Long dictionaryId;

    /** Class of the current dictionary. */
    protected String dictionaryClass;

    /** Size of the current dictionary. */
    protected Integer dictionarySize;

    /** Order of the current dictionary (i.e. base-2 log of its size). */
    protected Integer dictionaryOrder;

    public DictionaryAttributes(DictionaryAttributes other) {
      this(
          other.getDictionaryId(),
          other.getDictionaryClass(),
          other.getDictionarySize(),
          other.getDictionaryOrder());
    }

    /** Returns an instance representing no dictionary (all attributes set to {@code null}. */
    public static DictionaryAttributes none() {
      return NONE;
    }
  }
}
