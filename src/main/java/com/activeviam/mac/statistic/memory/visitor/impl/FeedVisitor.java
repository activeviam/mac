/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.statistic.memory.visitor.impl;

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
import com.qfs.store.IRecordSet;
import com.qfs.store.record.IRecordFormat;
import com.qfs.store.transaction.IOpenedTransaction;

import java.time.Instant;
import java.util.Objects;
import java.util.logging.Logger;

import static com.qfs.monitoring.statistic.memory.MemoryStatisticConstants.ATTR_NAME_CREATOR_CLASS;
import static com.qfs.monitoring.statistic.memory.MemoryStatisticConstants.ATTR_NAME_DICTIONARY_ID;

public class FeedVisitor implements IMemoryStatisticVisitor<Void> {

	/** Type name if the memory usage comes from an {@link IRecordSet} */
	public static final String TYPE_RECORD = "Record";
	/** Type name if the memory usage comes from references */
	public static final String TYPE_REFERENCE = "Reference";
	/** Type name if the memory usage comes from indexes */
	public static final String TYPE_INDEX = "Index";
	/** Type name if the memory usage comes from dictionaries */
	public static final String TYPE_DICTIONARY = "Dictionary";
	/** Type name if the memory usage comes from the version chunks */
	public static final String TYPE_VERSION_COLUMN = "VersionColumn";
	/** Class logger */
	private static final Logger logger = Logger.getLogger(Loggers.LOADING);

	private final IDatastoreSchemaMetadata storageMetadata;
	private final IOpenedTransaction transaction;
	private final String dumpName;

	/** The export date, found on the first statistics we read */
	protected Instant current = null;
	/** The epoch id we are currently reading statistics for */
	protected Long epochId = null;
	protected String branch = null;

	public FeedVisitor(
			final IDatastoreSchemaMetadata storageMetadata,
			final IOpenedTransaction tm,
			final String dumpName) {
		this.storageMetadata = storageMetadata;
		this.transaction = tm;
		this.dumpName = dumpName;
	}

	static Object[] buildChunkTupleFrom(
			final IRecordFormat format,
			final ChunkStatistic stat) {
		final Object[] tuple = new Object[format.getFieldCount()];

		tuple[format.getFieldIndex(DatastoreConstants.CHUNK_ID)] = stat.getChunkId();
		tuple[format.getFieldIndex(DatastoreConstants.CHUNK__CLASS)] = stat.getAttribute(ATTR_NAME_CREATOR_CLASS).asText();
		tuple[format.getFieldIndex(DatastoreConstants.CHUNK__OFF_HEAP_SIZE)] = stat.getShallowOffHeap();
		tuple[format.getFieldIndex(DatastoreConstants.CHUNK__ON_HEAP_SIZE)] = stat.getShallowOnHeap();
		final IStatisticAttribute fieldAttr = stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_FIELD);
		if (fieldAttr != null) {
			tuple[format.getFieldIndex(DatastoreConstants.CHUNK__FIELD)] = fieldAttr.asText();
		}

		return tuple;
	}

	static Object[] buildDictionaryTupleFrom(
			final IRecordFormat format,
			final IMemoryStatistic stat) {
		final Object[] tuple = new Object[format.getFieldCount()];

		final IStatisticAttribute dicIdAttr = Objects.requireNonNull(
				stat.getAttribute(ATTR_NAME_DICTIONARY_ID),
				() -> "No dictionary ID for " + stat);
		tuple[format.getFieldIndex(DatastoreConstants.DICTIONARY_ID)] = dicIdAttr.asLong();

		// TODO(ope) deal with dictionary fields if any
//		final IStatisticAttribute fieldNamesAttr = stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_FIELDS);
//		if (fieldNamesAttr!=null) {
//			currentChunkRecord[chunkRecordFormat.getFieldIndex(DatastoreConstants.FIELDS)] = new StringArrayObject(
//					fieldNamesAttr.asStringArray());
//		}

		final IStatisticAttribute dicClassAttr = stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_CLASS);
		final String dictionaryClass;
		if (dicClassAttr != null) {
			dictionaryClass = dicClassAttr.asText();
		} else {
			logger.warning("Dictionary does not state its class " + stat);
			dictionaryClass = stat.getAttribute(ATTR_NAME_CREATOR_CLASS).asText();
		}
		tuple[format.getFieldIndex(DatastoreConstants.DICTIONARY_CLASS)] = dictionaryClass;

		tuple[format.getFieldIndex(DatastoreConstants.DICTIONARY_SIZE)] =
				stat.getAttribute(DatastoreConstants.DICTIONARY_SIZE).asInt();
		tuple[format.getFieldIndex(DatastoreConstants.DICTIONARY_ORDER)] =
				stat.getAttribute(DatastoreConstants.DICTIONARY_ORDER).asInt();

		return tuple;
	}

	static Object[] buildChunksetTupleFrom(
			final IRecordFormat format,
			final ChunkSetStatistic stat) {
		final Object[] tuple = new Object[format.getFieldCount()];

		IStatisticAttribute chunkSetIdAttr = Objects.requireNonNull(
				stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_CHUNKSET_ID),
				() -> "No id in this chunkset statistic: " + stat);
		tuple[format.getFieldIndex(DatastoreConstants.CHUNKSET_ID)] = chunkSetIdAttr.asLong();

		tuple[format.getFieldIndex(DatastoreConstants.CHUNK_SET_CLASS)] =
				stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_CLASS).asText();
		tuple[format.getFieldIndex(DatastoreConstants.CHUNK_SET_FREE_ROWS)] = 0;
		tuple[format.getFieldIndex(DatastoreConstants.CHUNKSET__FREED_ROWS)] =
				stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_FREED_ROWS).asInt();
		tuple[format.getFieldIndex(DatastoreConstants.CHUNK_SET_PHYSICAL_CHUNK_SIZE)] =
				stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_LENGTH).asInt();

		return tuple;
	}

	@Override
	public Void visit(DefaultMemoryStatistic stat) {
		switch (stat.getName()) {
		case MemoryStatisticConstants.STAT_NAME_DATASTORE:
		case MemoryStatisticConstants.STAT_NAME_STORE:
			final DatastoreFeederVisitor datastoreFeed = new DatastoreFeederVisitor(
					this.storageMetadata,
					this.transaction,
					this.dumpName);
			datastoreFeed.startFrom(stat);
			break;
		case PivotMemoryStatisticConstants.STAT_NAME_MANAGER:
		case PivotMemoryStatisticConstants.STAT_NAME_PIVOT:
			final PivotFeederVisitor feed = new PivotFeederVisitor(
					this.storageMetadata,
					this.transaction,
					this.dumpName);
			feed.startFrom(stat);
			break;
		default:
			if (stat.getChildren() != null) {
				for (final IMemoryStatistic child : stat.getChildren()) {
					child.accept(this);
				}
			}
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

	static void includeApplicationInfoIfAny(
			final IOpenedTransaction tm,
			final Instant date,
			final long epoch,
			final String dumpName,
			final IMemoryStatistic stat) {
		final IStatisticAttribute usedHeap = stat.getAttribute(MemoryStatisticConstants.STAT_NAME_GLOBAL_USED_HEAP_MEMORY);
		final IStatisticAttribute maxHeap = stat.getAttribute(MemoryStatisticConstants.ST$AT_NAME_GLOBAL_MAX_HEAP_MEMORY);
		final IStatisticAttribute usedOffHeap = stat.getAttribute(MemoryStatisticConstants.STAT_NAME_GLOBAL_USED_DIRECT_MEMORY);
		final IStatisticAttribute maxOffHeap = stat.getAttribute(MemoryStatisticConstants.STAT_NAME_GLOBAL_MAX_DIRECT_MEMORY);

		if (usedHeap != null && maxHeap != null && usedOffHeap != null && maxOffHeap != null) {
			tm.add(
					DatastoreConstants.APPLICATION_STORE,
					date,
					epoch,
					dumpName,
					usedHeap.asLong(),
					maxHeap.asLong(),
					usedOffHeap.asLong(),
					maxOffHeap.asLong());
		} else {
			throw new RuntimeException("Missing memory properties in " + stat.getName() + ": " + stat.getAttributes());
		}
	}

	static IRecordFormat getRecordFormat(
			final IDatastoreSchemaMetadata storageMetadata,
			final String storeName) {
		return storageMetadata.getStoreMetadata(storeName)
				.getStoreFormat()
				.getRecordFormat();
	}

	static IRecordFormat getDictionaryFormat(IDatastoreSchemaMetadata storageMetadata) {
		return getRecordFormat(storageMetadata, DatastoreConstants.DICTIONARY_STORE);
	}

	static IRecordFormat getChunksetFormat(IDatastoreSchemaMetadata storageMetadata) {
		return getRecordFormat(storageMetadata, DatastoreConstants.CHUNKSET_STORE);
	}

}
