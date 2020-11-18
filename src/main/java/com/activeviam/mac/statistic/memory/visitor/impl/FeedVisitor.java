/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

import static com.qfs.monitoring.statistic.memory.MemoryStatisticConstants.ATTR_NAME_CREATOR_CLASS;
import static com.qfs.monitoring.statistic.memory.MemoryStatisticConstants.ATTR_NAME_DICTIONARY_ID;

import com.activeviam.mac.Loggers;
import com.activeviam.mac.memory.DatastoreConstants;
import com.qfs.monitoring.statistic.IStatisticAttribute;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.PivotMemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.impl.ChunkSetStatistic;
import com.qfs.monitoring.statistic.memory.impl.ChunkStatistic;
import com.qfs.monitoring.statistic.memory.impl.DefaultMemoryStatistic;
import com.qfs.monitoring.statistic.memory.impl.DictionaryStatistic;
import com.qfs.monitoring.statistic.memory.impl.IndexStatistic;
import com.qfs.monitoring.statistic.memory.impl.ReferenceStatistic;
import com.qfs.monitoring.statistic.memory.visitor.IMemoryStatisticVisitor;
import com.qfs.store.IDatastoreSchemaMetadata;
import com.qfs.store.record.IRecordFormat;
import com.qfs.store.transaction.IOpenedTransaction;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.logging.Logger;

/**
 * Visitor for chunks.
 *
 * @author ActiveViam
 */
public class FeedVisitor implements IMemoryStatisticVisitor<Void> {

  /** Class logger. */
  private static final Logger logger = Logger.getLogger(Loggers.LOADING);

  private final IDatastoreSchemaMetadata storageMetadata;
  private final IOpenedTransaction transaction;
  private final String dumpName;

  /**
   * Contructor.
   *
   * @param storageMetadata metadata of the Datastore schema
   * @param tm ongoing transaction
   * @param dumpName name of the import being currently executed
   */
  public FeedVisitor(
      final IDatastoreSchemaMetadata storageMetadata,
      final IOpenedTransaction tm,
      final String dumpName) {
    this.storageMetadata = storageMetadata;
    this.transaction = tm;
    this.dumpName = dumpName;
  }

  /**
   * Adds a tuple to the current transaction.
   *
   * @param statistic statistic
   * @param transaction ongoing transaction
   * @param store store being appended
   * @param tuple tuple-ized data to add to the store
   */
  protected static void add(
      IMemoryStatistic statistic, IOpenedTransaction transaction, String store, Object... tuple) {
    transaction.add(store, tuple);
  }

  static Object[] buildChunkTupleFrom(final IRecordFormat format, final ChunkStatistic stat) {
    final Object[] tuple = new Object[format.getFieldCount()];
    tuple[format.getFieldIndex(DatastoreConstants.CHUNK_ID)] = stat.getChunkId();
    tuple[format.getFieldIndex(DatastoreConstants.CHUNK__CLASS)] =
        stat.getAttribute(ATTR_NAME_CREATOR_CLASS).asText();
    tuple[format.getFieldIndex(DatastoreConstants.CHUNK__OFF_HEAP_SIZE)] = stat.getShallowOffHeap();
    tuple[format.getFieldIndex(DatastoreConstants.CHUNK__ON_HEAP_SIZE)] = stat.getShallowOnHeap();

    final IStatisticAttribute sizeAttribute =
        stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_LENGTH);
    final int chunkSize = sizeAttribute == null ? 0 : sizeAttribute.asInt();
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__SIZE, chunkSize);

    // TODO(ope) we may want to do the same for chunk whenever possible
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__NON_WRITTEN_ROWS, 0);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__FREE_ROWS, 0);

    return tuple;
  }

  /**
   * Writes a record into the {@link DatastoreConstants#CHUNK_STORE} for each given field using the
   * given tuple as a base.
   *
   * <p>This method can modify the {@link DatastoreConstants#OWNER__FIELD} column of the given
   * tuple.
   *
   * @param statistic the statistic associated with the chunk
   * @param transaction the ongoing transaction
   * @param fields the fields associated with the chunk statistic
   * @param format the format of the tuple
   * @param tuple the base tuple to write records with
   */
  static void writeChunkTupleForFields(
      final ChunkStatistic statistic, final IOpenedTransaction transaction,
      final Collection<String> fields, final IRecordFormat format, final Object... tuple) {
    if (fields == null || fields.isEmpty()) {
      FeedVisitor.add(statistic, transaction, DatastoreConstants.CHUNK_STORE, tuple);
    } else {
      fields.forEach(field -> {
        FeedVisitor
            .setTupleElement(tuple, format, DatastoreConstants.OWNER__FIELD, field);
        FeedVisitor.add(statistic, transaction, DatastoreConstants.CHUNK_STORE, tuple);
      });
    }
  }

  static Object[] buildDictionaryTupleFrom(
      final IRecordFormat format, final IMemoryStatistic stat) {
    final Object[] tuple = new Object[format.getFieldCount()];

    final IStatisticAttribute dicIdAttr =
        Objects.requireNonNull(
            stat.getAttribute(ATTR_NAME_DICTIONARY_ID), () -> "No dictionary ID for " + stat);
    tuple[format.getFieldIndex(DatastoreConstants.DICTIONARY_ID)] = dicIdAttr.asLong();

    final IStatisticAttribute dicClassAttr =
        stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_CLASS);
    final String dictionaryClass;
    if (dicClassAttr != null) {
      dictionaryClass = dicClassAttr.asText();
    } else {
      logger.warning("Dictionary does not state its class " + stat);
      dictionaryClass = stat.getAttribute(ATTR_NAME_CREATOR_CLASS).asText();
    }
    //
    tuple[format.getFieldIndex(DatastoreConstants.DICTIONARY_CLASS)] = dictionaryClass;
    tuple[format.getFieldIndex(DatastoreConstants.DICTIONARY_SIZE)] =
        stat.getAttribute(DatastoreConstants.DICTIONARY_SIZE).asInt();
    tuple[format.getFieldIndex(DatastoreConstants.DICTIONARY_ORDER)] =
        stat.getAttribute(DatastoreConstants.DICTIONARY_ORDER).asInt();

    return tuple;
  }

  static Object[] buildVersionTupleFrom(
      final IRecordFormat format, final IMemoryStatistic statistic,
      final String dumpName, final long epochId, final String branch) {
    final Object[] tuple = new Object[format.getFieldCount()];
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.VERSION__DUMP_NAME,
        dumpName);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.VERSION__EPOCH_ID,
        epochId);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.VERSION__BRANCH_NAME,
        branch);
    return tuple;
  }

  @Override
  public Void visit(DefaultMemoryStatistic stat) {
    switch (stat.getName()) {
      case MemoryStatisticConstants.STAT_NAME_DATASTORE:
      case MemoryStatisticConstants.STAT_NAME_MULTIVERSION_STORE:
      case MemoryStatisticConstants.STAT_NAME_STORE:
        final DatastoreFeederVisitor visitor =
            new DatastoreFeederVisitor(
                this.storageMetadata, this.transaction, this.dumpName);
        visitor.startFrom(stat);
        break;
      case PivotMemoryStatisticConstants.STAT_NAME_MANAGER:
      case PivotMemoryStatisticConstants.STAT_NAME_MULTIVERSION_PIVOT:
      case PivotMemoryStatisticConstants.STAT_NAME_PIVOT:
        final PivotFeederVisitor feed =
            new PivotFeederVisitor(
                this.storageMetadata, this.transaction, this.dumpName);
        feed.startFrom(stat);
        break;
      default:
        visitChildren(this, stat);
    }

    return null;
  }

  @Override
  public Void visit(ChunkSetStatistic chunkSetStatistic) {
    return null;
  }

  @Override
  public Void visit(ChunkStatistic chunkStatistic) {
    return null;
  }

  @Override
  public Void visit(ReferenceStatistic referenceStatistic) {
    return null;
  }

  @Override
  public Void visit(IndexStatistic indexStatistic) {
    return null;
  }

  @Override
  public Void visit(DictionaryStatistic dictionaryStatistic) {
    return null;
  }

  /**
   * Visits all the children of the given {@link IMemoryStatistic}.
   *
   * @param visitor the visitor to use to visit the children
   * @param statistic The statistics whose children to visit
   */
  protected static void visitChildren(
      final IMemoryStatisticVisitor<?> visitor, final IMemoryStatistic statistic) {
    if (statistic.getChildren() != null) {
      for (final IMemoryStatistic child : statistic.getChildren()) {
        child.accept(visitor);
      }
    }
  }

  static void includeApplicationInfoIfAny(
      final IOpenedTransaction tm,
      final Instant date,
      final String dumpName,
      final IMemoryStatistic stat) {
    final IStatisticAttribute usedHeap =
        stat.getAttribute(MemoryStatisticConstants.STAT_NAME_GLOBAL_USED_HEAP_MEMORY);
    final IStatisticAttribute maxHeap =
        stat.getAttribute(MemoryStatisticConstants.ST$AT_NAME_GLOBAL_MAX_HEAP_MEMORY);
    final IStatisticAttribute usedOffHeap =
        stat.getAttribute(MemoryStatisticConstants.STAT_NAME_GLOBAL_USED_DIRECT_MEMORY);
    final IStatisticAttribute maxOffHeap =
        stat.getAttribute(MemoryStatisticConstants.STAT_NAME_GLOBAL_MAX_DIRECT_MEMORY);

    if (usedHeap != null && maxHeap != null && usedOffHeap != null && maxOffHeap != null) {
      tm.add(
          DatastoreConstants.APPLICATION_STORE,
          dumpName,
          date,
          usedHeap.asLong(),
          maxHeap.asLong(),
          usedOffHeap.asLong(),
          maxOffHeap.asLong());
    } else {
      throw new RuntimeException(
          "Missing memory properties in " + stat.getName() + ": " + stat.getAttributes());
    }
  }

  static IRecordFormat getRecordFormat(
      final IDatastoreSchemaMetadata storageMetadata, final String storeName) {
    return storageMetadata.getStoreMetadata(storeName).getStoreFormat().getRecordFormat();
  }

  /**
   * Adds a value to a given field of a tuple.
   *
   * @param tuple tuple to be filled
   * @param format {@link IRecordFormat} the tuple must match
   * @param field field we're inserting data on
   * @param value data to insert
   */
  protected static void setTupleElement(
      Object[] tuple, IRecordFormat format, String field, final Object value) {
    if (value == null && format.isPrimitive(format.getFieldIndex(field))) {
      throw new RuntimeException("Expected a non-null value for field " + field);
    }
    tuple[format.getFieldIndex(field)] = value;
  }

  /**
   * Checks the format of a tuple correspond to the given input.
   *
   * @param tuple tuple to be checked
   * @param format format the tuple must match
   */
  protected static void checkTuple(Object[] tuple, IRecordFormat format) {
    for (int i = 0; i < tuple.length; i++) {
      if (tuple[i] == null) {
        String field = format.getFieldName(i);
        throw new RuntimeException(
            "Unexpected null value for field " + field + " in tuple: " + Arrays.toString(tuple));
      }
    }
  }
}
