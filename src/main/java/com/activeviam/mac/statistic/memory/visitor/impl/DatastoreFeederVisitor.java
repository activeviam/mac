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
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription.ParentType;
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
import com.qfs.store.record.IRecordFormat;
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
 * @author ActiveViam
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
	private final IOpenedTransaction transaction;
	/** The record format of the store that stores the chunks. */
	protected final IRecordFormat chunkRecordFormat;
	/** The name of the off-heap dump. Can be null */
	protected final String dumpName;

	/** The export date, found on the first statistics we read */
	protected Instant current = null;
	/** The epoch id we are currently reading statistics for */
	protected Long epochId = null;
	/** Branch owning {@link #epochId}. */
	protected String branch = null;

	/** ID of the current {@link ChunkSet}. */
	protected Long chunkSetId = null;
	/** ID of the current reference. */
	protected Long referenceId = null;
	/** ID of the current index. */
	protected Long indexId = null;
	/** ID of the current dictionary. */
	protected Long dictionaryId = null;

	protected ParentType directParentType;
	protected String directParentId;

	/** The name of the store of the visited statistic */
	protected Integer partitionId = null;
	/** The partition id of the visited statistic */
	protected String store = null;
	protected String field = null;
	private IndexType indexType = null;

	protected StatisticTreePrinter printer;

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
		this.transaction = transaction;
		this.chunkRecordFormat = this.storageMetadata
				.getStoreMetadata(DatastoreConstants.CHUNK_STORE)
				.getStoreFormat()
				.getRecordFormat();
		this.dumpName = dumpName;
	}

	public void startFrom(final IMemoryStatistic stat) {
		this.printer = DebugVisitor.createDebugPrinter(stat);
		if (this.current == null) {
			final IStatisticAttribute dateAtt = stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_DATE);
			if (dateAtt == null) {
				throw new IllegalStateException("First level statistic should contain the export date.");
			}
			this.current = Instant.ofEpochSecond(dateAtt.asLong());

			readEpochAndBranchIfAny(stat);
			assert this.epochId != null;

			FeedVisitor.includeApplicationInfoIfAny(
					this.transaction,
					this.current,
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

		FeedVisitor.setTupleElement(tuple, chunkRecordFormat, DatastoreConstants.CHUNK__PARENT_TYPE, this.directParentType);
		FeedVisitor.setTupleElement(tuple, chunkRecordFormat, DatastoreConstants.CHUNK__PARENT_ID, this.directParentId);

		FeedVisitor.setTupleElement(tuple, chunkRecordFormat, DatastoreConstants.CHUNK__OWNER, this.store);
//		FeedVisitor.setTupleElement(tuple, chunkRecordFormat, DatastoreConstants.CHUNK__COMPONENT, this.ownerComponent);
		tuple[chunkRecordFormat.getFieldIndex(DatastoreConstants.CHUNK__PARTITION_ID)] = this.partitionId;

		tuple[chunkRecordFormat.getFieldIndex(DatastoreConstants.CHUNK__DEBUG_TREE)] = StatisticTreePrinter.getTreeAsString(chunkStatistic);

		/* // FIXME(ope) restore this if needed
		if (this.store != null) {
			final IByteRecordFormat f = this.storageMetadata.getStoreMetadata(DatastoreConstants.CHUNK_AND_STORE__STORE_NAME).getStoreFormat().getRecordFormat();
			final Object[] tupleChunkAndStore = FeedVisitor.buildChunkAndStoreTuple(f, chunkStatistic, this.store);
			FeedVisitor.setTupleElement(tupleChunkAndStore, f, DatastoreConstants.CHUNK_AND_STORE__STORE, this.store);
			FeedVisitor.add(chunkStatistic, this.transaction, DatastoreConstants.CHUNK_AND_STORE__STORE_NAME, tupleChunkAndStore);
		}
		//*/

		FeedVisitor.add(chunkStatistic, this.transaction, DatastoreConstants.CHUNK_STORE, tuple);

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
		case MemoryStatisticConstants.STAT_NAME_RECORD_SET:
			processRecords(stat);
			break;
		case MemoryStatisticConstants.STAT_NAME_VERSIONS_COLUMN:
			processRecordVersion(stat);
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
		return new ChunkSetStatisticVisitor(
				this.storageMetadata,
				this.transaction,
				this.dumpName,
				this.current,
				this.store,
				this.partitionId
		).visit(stat);
	}

	@Override
	public Void visit(final ReferenceStatistic referenceStatistic) {
		final Object[] tuple = buildPartOfReferenceStatisticTuple(this.storageMetadata, referenceStatistic);
		final IRecordFormat refStoreFormat = getReferenceFormat(this.storageMetadata);

		// fill out the tuple
		this.referenceId = referenceStatistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_REFERENCE_ID).asLong();

		tuple[refStoreFormat.getFieldIndex(DatastoreConstants.REFERENCE_ID)] = this.referenceId;
		tuple[refStoreFormat.getFieldIndex(DatastoreConstants.REFERENCE_NAME)] = referenceStatistic.getAttribute(DatastoreConstants.REFERENCE_NAME).asText();
		tuple[refStoreFormat.getFieldIndex(DatastoreConstants.REFERENCE_CLASS)] = referenceStatistic.getAttribute(DatastoreConstants.REFERENCE_CLASS).asText();
		FeedVisitor.add(referenceStatistic, this.transaction, DatastoreConstants.REFERENCE_STORE, tuple);

		final ParentType previousParentType = this.directParentType;
		final String previousParentId = this.directParentId;
		this.directParentType = ParentType.REFERENCE;
		this.directParentId = String.valueOf(this.referenceId);
		visitChildren(this, referenceStatistic);
		this.directParentType = previousParentType;
		this.directParentId = previousParentId;

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

		FeedVisitor.add(stat, this.transaction, DatastoreConstants.INDEX_STORE, tuple);

		final ParentType previousParentType = this.directParentType;
		final String previousParentId = this.directParentId;
		this.directParentType = ParentType.INDEX;
		this.directParentId = String.valueOf(this.indexId);
		visitChildren(this, stat);
		this.directParentType = previousParentType;
		this.directParentId = previousParentId;

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

		FeedVisitor.add(stat, this.transaction, DatastoreConstants.DICTIONARY_STORE, tuple);

		final IStatisticAttribute fieldAttr = stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_FIELD);
		if (fieldAttr != null) {
			this.field = fieldAttr.asText();
		}

		final ParentType previousParentType = this.directParentType;
		final String previousParentId = this.directParentId;
		this.directParentType = ParentType.DICTIONARY;
		this.directParentId = String.valueOf(this.dictionaryId);
		visitChildren(this, stat);
		this.directParentType = previousParentType;
		this.directParentId = previousParentId;

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

	private void processRecords(final IMemoryStatistic stat) {
		final ParentType previousParentType = this.directParentType;
		final String previousParentId = this.directParentId;

		this.directParentType = ParentType.RECORDS;
		this.directParentId = String.valueOf(
				stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_RECORD_SET_ID).asLong());
		visitChildren(this, stat);
		this.directParentType = previousParentType;
		this.directParentId = previousParentId;
	}

	private void processRecordVersion(final IMemoryStatistic stat) {
		visitChildren(this, stat);
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
		isVersionColumn = MemoryStatisticConstants.STAT_NAME_VERSIONS_COLUMN.equals(stat.getName());

		visitChildren(this, stat);

		isVersionColumn = false;
	}

	protected String getType(ChunkStatistic statistic) {
		if (this.dictionaryId != null) {
			return FeedVisitor.TYPE_DICTIONARY; // priority to dic.
		} else if (this.chunkSetId != null) {
			return FeedVisitor.TYPE_RECORD;
		} else if (this.referenceId != null) {
			return FeedVisitor.TYPE_REFERENCE;
		} else if (this.indexId != null) {
			return FeedVisitor.TYPE_INDEX;
		} else if (this.isVersionColumn) {
			return FeedVisitor.TYPE_VERSION_COLUMN;
		} else {
			throw new RuntimeException("Cannot determine the type of " + statistic);
		}
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

	private static IRecordFormat getIndexFormat(IDatastoreSchemaMetadata storageMetadata) {
		return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.INDEX_STORE);
	}

	private static IRecordFormat getReferenceFormat(IDatastoreSchemaMetadata storageMetadata) {
		return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.REFERENCE_STORE);
	}

}
