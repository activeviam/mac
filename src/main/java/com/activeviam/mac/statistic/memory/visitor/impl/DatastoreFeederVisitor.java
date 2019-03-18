/*
 * (C) Quartet FS 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.statistic.memory.visitor.impl;

import com.activeviam.mac.Loggers;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription;
import com.qfs.monitoring.statistic.IStatisticAttribute;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.impl.ChunkSetStatistic;
import com.qfs.monitoring.statistic.memory.impl.ChunkStatistic;
import com.qfs.monitoring.statistic.memory.impl.DefaultMemoryStatistic;
import com.qfs.monitoring.statistic.memory.impl.DictionaryStatistic;
import com.qfs.monitoring.statistic.memory.impl.IndexStatistic;
import com.qfs.monitoring.statistic.memory.impl.ReferenceStatistic;
import com.qfs.monitoring.statistic.memory.visitor.IMemoryStatisticVisitor;
import com.qfs.store.IDatastoreSchemaMetadata;
import com.qfs.store.impl.ChunkSet;
import com.qfs.store.query.ICompiledGetByKey;
import com.qfs.store.record.IRecordFormat;
import com.qfs.store.record.IRecordReader;
import com.qfs.store.transaction.IOpenedTransaction;

import java.time.Instant;
import java.util.Objects;
import java.util.logging.Logger;

import static com.activeviam.mac.statistic.memory.visitor.impl.FeedVisitor.visitChildren;


/**
 * This visitor is not reusable.
 * <p>
 * This visits memory statistics and commits them into the given datastore. The datastore must have
 * the schema defined by {@link MemoryAnalysisDatastoreDescription}.
 *
 * @author Quartet FS
 */
public class DatastoreFeederVisitor implements IMemoryStatisticVisitor<Void> {

	/** Class logger */
	private static final Logger logger = Logger.getLogger(Loggers.DATASTORE_LOADING);

	/**
	 * A boolean that if true tells us that the currently visited component is responsible for
	 * storing versioning data.
	 */
	protected boolean isVersionColumn = false; // FIXME find a cleaner way to do that. (specific stat for instance).

	private final IDatastoreSchemaMetadata storageMetadata;
	private final IOpenedTransaction tm;
	/** The record format of the store that stores the chunks. */
	protected final IRecordFormat chunkRecordFormat;
	/** The name of the off-heap dump. Can be null */
	protected final String dumpName;

//	/** The current record, that will be added to the transaction manager when visiting a leaf. */
//	protected Object[] currentChunkRecord;
	/** The export date, found on the first statistics we read */
	protected Instant current = null;
	/** The epoch id we are currently reading statistics for */
	protected Long epochId = null;
	protected String branch = null;

	/** ID of the current {@link ChunkSet}. */
	protected Long chunkSetId = null;
	/** ID of the current reference. */
	protected Long referenceId = null;
	/** ID of the current index. */
	protected Long indexId = null;
	/** ID of the current dictionary. */
	protected Long dictionaryId = null;

	/** The name of the store of the visited statistic */
	protected Integer partitionId = null;
	/** The partition id of the visited statistic */
	protected String store = null;
	protected String field = null;
	private IndexType indexType = null;

	protected StatisticTreePrinter printer;
	protected final ICompiledGetByKey compiledGetByKeyDictionaryStore;

	/**
	 * Constructor.
	 *
	 * @param storageMetadata structure of the metadata
	 * @param transaction the open transaction to use to fill the datastore with the visited data
	 * @param dumpName The name of the off-heap dump. Can be null.
	 */
	public DatastoreFeederVisitor(
			final IDatastoreSchemaMetadata storageMetadata,
			final IOpenedTransaction transaction,
			final String dumpName) {
		this.storageMetadata = storageMetadata;
		this.tm = transaction;
		this.chunkRecordFormat = this.storageMetadata
				.getStoreMetadata(DatastoreConstants.CHUNK_STORE)
				.getStoreFormat()
				.getRecordFormat();
		this.dumpName = dumpName;

		if (this.storageMetadata.getKeyFields(DatastoreConstants.CHUNK_STORE).size() > 1) {
			throw new RuntimeException("Expected only one key field.");
		}
		this.compiledGetByKeyDictionaryStore = this.tm.getQueryRunner().createGetByKeyQuery(DatastoreConstants.DICTIONARY_STORE, DatastoreConstants.DICTIONARY_IS_LEVEL);
	}

	private static Object[] buildIndexTupleFrom(
			final IRecordFormat format,
			final IndexStatistic stat) {
		final Object[] tuple = new Object[format.getFieldCount()];
		tuple[format.getFieldIndex(DatastoreConstants.INDEX_ID)] = stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_INDEX_ID).asLong();

		final String[] fieldNames = stat.getAttribute(DatastoreConstants.FIELDS).asStringArray();
		assert fieldNames != null && fieldNames.length > 0 : "Cannot find fields in the attributes of " + stat;
		tuple[format.getFieldIndex(DatastoreConstants.INDEX__FIELDS)] = new MemoryAnalysisDatastoreDescription.StringArrayObject(fieldNames);

		tuple[format.getFieldIndex(DatastoreConstants.INDEX_CLASS)] = stat.getAttribute(DatastoreConstants.INDEX_CLASS).asText();

		return tuple;
	}

	private static IRecordFormat getIndexFormat(IDatastoreSchemaMetadata storageMetadata) {
		return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.INDEX_STORE);
	}

	public void startFrom(final IMemoryStatistic stat) {
		this.printer = DebugVisitor.createDebugPrinter(stat);
		if (this.current == null) {
			final IStatisticAttribute dateAtt = stat.getAttribute(DatastoreConstants.CHUNK__EXPORT_DATE);
			if (dateAtt == null) {
				throw new IllegalStateException("First level statistic should contain the export date.");
			}
			this.current = Instant.ofEpochSecond(dateAtt.asLong());

			readEpochAndBranchIfAny(stat);
			assert this.epochId != null;

			FeedVisitor.includeApplicationInfoIfAny(
					this.tm,
					this.current,
					this.epochId,
					this.dumpName,
					stat);

			try {
				stat.accept(this);
			} finally {
				this.printer.print();
			}
		} else {
			throw new RuntimeException("Cannot reuse a feed instance");
		}
	}

	@Override
	public Void visit(final ChunkStatistic chunkStatistic) {
		final Object[] tuple = FeedVisitor.buildChunkTupleFrom(this.chunkRecordFormat, chunkStatistic);
		FeedVisitor.setTupleElement(tuple, chunkRecordFormat, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);
		FeedVisitor.setTupleElement(tuple, chunkRecordFormat, DatastoreConstants.CHUNK__EXPORT_DATE, this.current);

		if (indexId != null && dictionaryId != null) {
			throw new RuntimeException();
		}

		final String type;
		final String group;
		if (this.chunkSetId != null) {
			tuple[chunkRecordFormat.getFieldIndex(DatastoreConstants.CHUNKSET_ID)] = this.chunkSetId;
			type = FeedVisitor.TYPE_RECORD;
			group = FeedVisitor.GROUP_DATA;
		} else if (referenceId != null) {
			tuple[chunkRecordFormat.getFieldIndex(DatastoreConstants.REFERENCE_ID)] = this.referenceId;
			type = FeedVisitor.TYPE_REFERENCE;
			group = FeedVisitor.GROUP_REFERENCE;
		} else if (indexId != null) {
			tuple[chunkRecordFormat.getFieldIndex(DatastoreConstants.INDEX_ID)] = this.indexId;
			type = FeedVisitor.TYPE_INDEX;
			group = FeedVisitor.GROUP_INDEX;
		} else if (dictionaryId != null) {
			tuple[chunkRecordFormat.getFieldIndex(DatastoreConstants.DICTIONARY_ID)] = this.dictionaryId;
			type = FeedVisitor.TYPE_DICTIONARY;
			String gr = FeedVisitor.GROUP_OTHER;
	;
			// No need to flush because of DuplicateKeyHandlers.THROW_WITHIN_TRANSACTION
			IRecordReader reader = this.compiledGetByKeyDictionaryStore.runInTransaction(new Object[] { dictionaryId }, false);
			if (reader != null) {
				if(reader.readBoolean(0)) { // Level dic.
					gr = FeedVisitor.GROUP_HIERARCHY;
				}
			}
			group = gr;
		} else if (isVersionColumn) {
			type = FeedVisitor.TYPE_VERSION_COLUMN;
			group = FeedVisitor.GROUP_DATA;
		} else {
			throw new RuntimeException("Cannot determine the type of " + chunkStatistic);
		}

		FeedVisitor.setTupleElement(tuple, chunkRecordFormat, DatastoreConstants.CHUNK__TYPE, type);
		FeedVisitor.setTupleElement(tuple, chunkRecordFormat, DatastoreConstants.CHUNK__GROUP, group);

//		if (group.equals(FeedVisitor.GROUP_OTHER)) {
		tuple[chunkRecordFormat.getFieldIndex(DatastoreConstants.CHUNK__DEBUG_TREE)] = StatisticTreePrinter.getTreeAsString(chunkStatistic);
//		}

		if (this.store != null) {
			FeedVisitor.setTupleElement(tuple, chunkRecordFormat, DatastoreConstants.CHUNK__PARTITION__STORE_NAME, this.store);
			if (this.partitionId != null) {
				// it can be null for the dictionary for instance
				FeedVisitor.setTupleElement(tuple, chunkRecordFormat, DatastoreConstants.CHUNK__PARTITION_ID, this.partitionId);
			}
		}

		if (this.field != null) {
			tuple[chunkRecordFormat.getFieldIndex(DatastoreConstants.CHUNK__FIELD)] = this.field;
		}

		tm.add(DatastoreConstants.CHUNK_STORE, tuple);

		visitChildren(this, chunkStatistic);

		return null;
	}

	@Override
	public Void visit(final DefaultMemoryStatistic stat) {
		final Long initialEpoch = this.epochId;
		final String initialBranch = this.branch;
		readEpochAndBranchIfAny(stat);

		switch (stat.getName()) {
		case MemoryStatisticConstants.STAT_NAME_STORE:
			processStoreStat(stat);
			break;
		case MemoryStatisticConstants.STAT_NAME_STORE_PARTITION:
			processStorePartition(stat);
			break;
		case MemoryStatisticConstants.STAT_NAME_PRIMARY_INDICES:
			processPrimaryIndices(stat);
			break;
		case MemoryStatisticConstants.STAT_NAME_SECONDARY_INDICES:
			processSecondaryIndices(stat);
			break;
		default:
			recordStatAndExplore(stat);
		}

		this.epochId = initialEpoch;
		this.branch = initialBranch;

		return null;
	}

	@Override
	public Void visit(final ChunkSetStatistic stat) {
		final IRecordFormat format = FeedVisitor.getChunksetFormat(this.storageMetadata);
		final Object[] tuple = FeedVisitor.buildChunksetTupleFrom(format, stat);

		assert this.epochId != null;
		final String type;
		if (this.partitionId != null) {
			// TODO cleaning not needed anymore
			type = FeedVisitor.TYPE_RECORD;
		} else if (this.dictionaryId != null) {
			tuple[format.getFieldIndex(DatastoreConstants.CHUNKSET__DICTIONARY_ID)] = this.dictionaryId;
			type = FeedVisitor.TYPE_DICTIONARY;
		} else {
			throw new RuntimeException("Cannot process this stat. A chunkset is not attached to a dictionary or a store. Faulty stat: " + stat);
		}
		tuple[format.getFieldIndex(DatastoreConstants.CHUNKSET__TYPE)] = type;

		tm.add(DatastoreConstants.CHUNKSET_STORE, tuple);

		this.chunkSetId = (Long) tuple[format.getFieldIndex(DatastoreConstants.CHUNKSET_ID)];
		if (stat.getChildren() != null) {
			for (final IMemoryStatistic child : stat.getChildren()) {
				// Could be the rowMapping if SparseChunkSet
//				final IStatisticAttribute keyAtt = child.getAttribute(MemoryStatisticConstants.ATTR_NAME_FIELD);
				// TODO(ope) use the field attributes of the chunkset
//				if (keyAtt != null) {
//					currentChunkRecord[chunkRecordFormat.getFieldIndex(DatastoreConstants.FIELDS)] =
//							new StringArrayObject(keyAtt.asText());
//				} else {
//					currentChunkRecord[chunkRecordFormat.getFieldIndex(DatastoreConstants.FIELDS)] = null;
//				}
				child.accept(this);
			}
		}
		// Reset
		this.chunkSetId = null;

		return null;
	}

	@Override
	public Void visit(final ReferenceStatistic referenceStatistic) {
		final Object[] tuple = buildPartOfReferenceStatisticTuple(this.storageMetadata, referenceStatistic);
		final IRecordFormat refStoreFormat = getReferenceFormat(this.storageMetadata);

		// fill out the tuple
		this.referenceId = referenceStatistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_REFERENCE_ID).asLong();

		tuple[refStoreFormat.getFieldIndex(DatastoreConstants.REFERENCE_ID)] = referenceId;
		tuple[refStoreFormat.getFieldIndex(DatastoreConstants.REFERENCE_NAME)] =
				referenceStatistic.getAttribute(DatastoreConstants.REFERENCE_NAME).asText();
		tuple[refStoreFormat.getFieldIndex(DatastoreConstants.REFERENCE_CLASS)] =
				referenceStatistic.getAttribute(DatastoreConstants.REFERENCE_CLASS).asText();
		tm.add(DatastoreConstants.REFERENCE_STORE, tuple);

		visitChildren(this, referenceStatistic);

		//Reset
		this.referenceId = null;

		return null;
	}

	@Override
	public Void visit(final IndexStatistic stat) {
		final IRecordFormat format = getIndexFormat(this.storageMetadata);
		final Object[] tuple = buildIndexTupleFrom(format, stat);

		this.indexId = stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_INDEX_ID).asLong();
		final boolean isKeyIndex = stat.getName().equals(MemoryStatisticConstants.STAT_NAME_KEY_INDEX);
		if (isKeyIndex) {
			this.indexType = IndexType.KEY;
		}

		FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.INDEX_TYPE, this.indexType);

		this.tm.add(DatastoreConstants.INDEX_STORE, tuple);

		visitChildren(this, stat);

		// Reset
		this.indexId = null;
		if (isKeyIndex) {
			this.indexType = null;
		}

		return null;
	}

	@Override
	public Void visit(final DictionaryStatistic stat) {
		final IRecordFormat format = FeedVisitor.getDictionaryFormat(this.storageMetadata);
		final Object[] tuple = FeedVisitor.buildDictionaryTupleFrom(format, stat);
		this.dictionaryId = (Long) tuple[format.getFieldIndex(DatastoreConstants.DICTIONARY_ID)];

		tm.add(DatastoreConstants.DICTIONARY_STORE, tuple);

		final IStatisticAttribute fieldAttr = stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_FIELD);
		if (fieldAttr != null) {
			this.field = fieldAttr.asText();
		}

		visitChildren(this, stat);

		// Reset
		this.dictionaryId = null;
		this.field = null;
		return null;
	}

	protected void processStoreStat(final IMemoryStatistic stat) {
		final IStatisticAttribute nameAttr = Objects.requireNonNull(
				stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_STORE_NAME),
				() -> "No store name in stat " + stat);
		this.store = nameAttr.asText();

		// Explore the store children
		visitChildren(this, stat);

		this.store = null;
	}

	protected void processStorePartition(final IMemoryStatistic stat) {
		final IStatisticAttribute partitionAttr = Objects.requireNonNull(
				stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_PARTITION_ID),
				() -> "No partition attribute in stat" + stat);
		this.partitionId = partitionAttr.asInt();

		visitChildren(this, stat);

		this.partitionId = null;
	}

	private void processPrimaryIndices(final IMemoryStatistic stat) {
		processIndexList(stat, IndexType.PRIMARY);
	}

	private void processSecondaryIndices(final IMemoryStatistic stat) {
		processIndexList(stat, IndexType.SECONDARY);
	}

	private void processIndexList(final IMemoryStatistic stat, final IndexType type) {
		this.indexType = type;
		visitChildren(this, stat);
		this.indexType = null;
	}

	private void recordStatAndExplore(final DefaultMemoryStatistic stat) {
//		if (stat.getAttributes().containsKey(DatastoreConstants.CHUNK__STORE_NAME)) {
//			currentChunkRecord = new Object[chunkRecordFormat.getFieldCount()];
//			// Set the store name
//			currentChunkRecord[chunkRecordFormat
//					.getFieldIndex(DatastoreConstants.CHUNK__STORE_NAME)] = stat.getAttributes()
//					.get(DatastoreConstants.CHUNK__STORE_NAME)
//					.asText();
//			currentChunkRecord[chunkRecordFormat.getFieldIndex(DatastoreConstants.CHUNK__EXPORT_DATE)] = current;
//		} else if (stat.getAttributes().containsKey(DatastoreConstants.CHUNK__PARTITION_ID)) {
//			// Set the partition id
//			currentChunkRecord[chunkRecordFormat
//					.getFieldIndex(DatastoreConstants.CHUNK__PARTITION_ID)] = stat.getAttributes()
//					.get(DatastoreConstants.CHUNK__PARTITION_ID)
//					.asInt();
//		}

		isVersionColumn = MemoryStatisticConstants.STAT_NAME_VERSIONS_COLUMN.equals(stat.getName());

		visitChildren(this, stat);

		isVersionColumn = false;

		// Reset
//		if (stat.getAttributes().containsKey(DatastoreConstants.CHUNK__STORE_NAME)) {
//			currentChunkRecord = null;
//		} else if (stat.getAttributes().containsKey(DatastoreConstants.CHUNK__PARTITION_ID)) {
//			currentChunkRecord[chunkRecordFormat.getFieldIndex(DatastoreConstants.CHUNK__PARTITION_ID)] = null;
//		}
	}

	private void readEpochAndBranchIfAny(final IMemoryStatistic stat) {
		final IStatisticAttribute epochAttr = stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_EPOCH);
		if (epochAttr != null) {
			this.epochId = epochAttr.asLong();
		}
		final IStatisticAttribute branchAttr = stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_BRANCH);
		if (branchAttr != null) {
			this.branch = branchAttr.asText();
		}
	}

	/**
	 * Build an object array that represents a reference.
	 *
	 * @param referenceStatistic the {@link ReferenceStatistic} from which the array must be built.
	 * @return the object array.
	 */
	protected Object[] buildPartOfReferenceStatisticTuple(
			final IDatastoreSchemaMetadata storageMetadata,
			final ReferenceStatistic referenceStatistic) {
		final IRecordFormat refStoreFormat = getReferenceFormat(storageMetadata);

		final Object[] tuple = new Object[refStoreFormat.getFieldCount()];
		tuple[refStoreFormat.getFieldIndex(
				DatastoreConstants.REFERENCE_FROM_STORE)] = referenceStatistic.getAttribute(DatastoreConstants.REFERENCE_FROM_STORE).asText();
		tuple[refStoreFormat.getFieldIndex(DatastoreConstants.REFERENCE_FROM_STORE_PARTITION_ID)] =
				referenceStatistic.getAttribute(DatastoreConstants.REFERENCE_FROM_STORE_PARTITION_ID).asInt();
		tuple[refStoreFormat.getFieldIndex(DatastoreConstants.REFERENCE_TO_STORE)] =
				referenceStatistic.getAttribute(DatastoreConstants.REFERENCE_TO_STORE).asText();
		tuple[refStoreFormat.getFieldIndex(DatastoreConstants.REFERENCE_TO_STORE_PARTITION_ID)] =
				referenceStatistic.getAttribute(DatastoreConstants.REFERENCE_TO_STORE_PARTITION_ID).asInt();

		return tuple;
	}

	private static IRecordFormat getReferenceFormat(IDatastoreSchemaMetadata storageMetadata) {
		return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.REFERENCE_STORE);
	}

}
