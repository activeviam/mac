/*
 * (C) ActiveViam 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.statistic.memory.visitor.impl;

import java.time.Instant;
import java.util.Objects;
import java.util.logging.Logger;

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
import com.qfs.store.IDatastoreSchemaMetadata;
import com.qfs.store.impl.ChunkSet;
import com.qfs.store.record.IRecordFormat;
import com.qfs.store.transaction.IOpenedTransaction;

/**
 * This visitor is not reusable.
 * <p>
 * This visits memory statistics and commits them into the given datastore. The
 * datastore must have the schema defined by
 * {@link MemoryAnalysisDatastoreDescription}.
 *
 * @author ActiveViam
 */
public class DatastoreFeederVisitor extends ADatastoreFeedVisitor<Void> {

	/** Class logger */
	private static final Logger logger = Logger.getLogger(Loggers.DATASTORE_LOADING);

	/**
	 * A boolean that if true tells us that the currently visited component is
	 * responsible for storing versioning data.
	 */
	protected boolean isVersionColumn = false; // FIXME find a cleaner way to do that. (specific stat for instance).

	/** The record format of the store that stores the chunks. */
	protected final IRecordFormat chunkRecordFormat;

	/** The export date, found on the first statistics we read */
	protected Instant current = null;
	/** The epoch id we are currently reading statistics for */
	protected Long epochId = null;
	/** Branch owning {@link #epochId}. */
	protected String branch = null;

	/** Type of the root component visited. */
	protected ParentType rootComponent;
	/** Types of the direct parent component owning the chunk. */
	protected ParentType directParentType;
	/** Id of the direct parent owning the chunk. */
	protected String directParentId;

	/** The partition id of the visited statistic */
	protected Integer partitionId = null;

	/** Type of the currently visited index. */
	private IndexType indexType = null;

	/** Printer displaying the tree of a given statistic. */
	protected StatisticTreePrinter printer;

	/**
	 * Constructor.
	 *
	 * @param storageMetadata structure of the metadata
	 * @param transaction     the open transaction to use to fill the datastore with
	 *                        the visited data
	 * @param dumpName        The name of the off-heap dump. Can be null.
	 */
	public DatastoreFeederVisitor(final IDatastoreSchemaMetadata storageMetadata,
			final IOpenedTransaction transaction,
			final String dumpName) {
		super(transaction, storageMetadata, dumpName);
		this.chunkRecordFormat = this.storageMetadata.
				getStoreMetadata(DatastoreConstants.CHUNK_STORE)
				.getStoreFormat()
				.getRecordFormat();
	}

	/**
	 * Starts navigating the tree of chunk statistics from the input entry point
	 *
	 * @param stat Entry point for the traversal of the memory statistics tree
	 */
	public void startFrom(final IMemoryStatistic stat) {
		this.printer = DebugVisitor.createDebugPrinter(stat);
		if (this.current == null) {
			final IStatisticAttribute dateAtt = stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_DATE);

			this.current = Instant.ofEpochSecond(null != dateAtt ? dateAtt.asLong() : System.currentTimeMillis());

			readEpochAndBranchIfAny(stat);
			if (!stat.getName().equalsIgnoreCase(MemoryStatisticConstants.STAT_NAME_MULTIVERSION_STORE)) {
				assert this.epochId != null;
			}

			FeedVisitor.includeApplicationInfoIfAny(this.transaction, this.current, this.dumpName, stat);

			try {
				stat.accept(this);
			} catch (Exception e) {
				this.printer.print();
				throw e;
			}
		} else {
			throw new RuntimeException("Cannot reuse a feed instance");
		}
	}

	@Override
	public Void visit(final ChunkStatistic chunkStatistic) {

		recordFieldForStructure(this.directParentType, this.directParentId);
		recordIndexForStructure(this.directParentType, this.directParentId);
		recordRefForStructure(this.directParentType, this.directParentId);

		final Object[] tuple = FeedVisitor.buildChunkTupleFrom(this.chunkRecordFormat, chunkStatistic);
		if (isVersionColumn) {
			FeedVisitor.setTupleElement(tuple, chunkRecordFormat, DatastoreConstants.CHUNK__NON_WRITTEN_ROWS,
					chunkStatistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_NOT_WRITTEN_ROWS).asInt());
		}
		FeedVisitor.setTupleElement(tuple, chunkRecordFormat, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);

		FeedVisitor.setTupleElement(tuple, chunkRecordFormat, DatastoreConstants.CHUNK__PARENT_TYPE,
				this.directParentType);
		FeedVisitor.setTupleElement(tuple, chunkRecordFormat, DatastoreConstants.CHUNK__PARENT_ID, this.directParentId);

		FeedVisitor.setTupleElement(tuple, chunkRecordFormat, DatastoreConstants.CHUNK__OWNER, this.store);
		FeedVisitor.setTupleElement(tuple, chunkRecordFormat, DatastoreConstants.CHUNK__COMPONENT, this.rootComponent);
		tuple[chunkRecordFormat.getFieldIndex(DatastoreConstants.CHUNK__PARTITION_ID)] = this.partitionId;
		tuple[chunkRecordFormat.getFieldIndex(DatastoreConstants.CHUNK__DEBUG_TREE)] = StatisticTreePrinter
				.getTreeAsString(chunkStatistic);

		FeedVisitor.add(chunkStatistic, this.transaction, DatastoreConstants.CHUNK_STORE, tuple);

		visitChildren(chunkStatistic);

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
		case MemoryStatisticConstants.STAT_NAME_DICTIONARY_MANAGER:
			processDictionaryManager(stat);
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
		return new ChunkSetStatisticVisitor(this.storageMetadata,
				this.transaction,
				this.dumpName,
				this.current,
				this.store,
				this.rootComponent,
				this.directParentType,
				this.directParentId,
				this.partitionId,
				this.indexId,
				this.referenceId).visit(stat);
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
		FeedVisitor.setTupleElement(tuple, refStoreFormat, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);

		FeedVisitor.add(referenceStatistic, this.transaction, DatastoreConstants.REFERENCE_STORE, tuple);

		final ParentType previousParentType = this.directParentType;
		final String previousParentId = this.directParentId;
		this.directParentType = ParentType.REFERENCE;
		this.directParentId = String.valueOf(this.referenceId);
		this.rootComponent = ParentType.REFERENCE;
		visitChildren(referenceStatistic);
		this.directParentType = previousParentType;
		this.directParentId = previousParentId;
		this.rootComponent = null;

		// Reset
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
		FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);

		FeedVisitor.add(stat, this.transaction, DatastoreConstants.INDEX_STORE, tuple);

		final ParentType previousParentType = this.directParentType;
		final String previousParentId = this.directParentId;
		this.directParentType = ParentType.INDEX;
		this.directParentId = String.valueOf(this.indexId);
		this.rootComponent = ParentType.INDEX;

		visitChildren(stat);

		this.directParentType = previousParentType;
		this.directParentId = previousParentId;
		this.rootComponent = null;

		// Reset
		this.indexId = null;
		if (isKeyIndex) {
			this.indexType = null;
		}

		return null;
	}

	@Override
	public Void visit(final DictionaryStatistic stat) {
		final IRecordFormat format = getDictionaryFormat(this.storageMetadata);
		final IRecordFormat joinStoreFormat = FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.CHUNK_TO_DICO_STORE);

		final Object[] tuple = FeedVisitor.buildDictionaryTupleFrom(format, stat);
		this.dictionaryId = (Long) tuple[format.getFieldIndex(DatastoreConstants.DICTIONARY_ID)];
		if (directParentId !=null && directParentType != null ) {
			FeedVisitor.add(
					stat,
					transaction,
					DatastoreConstants.CHUNK_TO_DICO_STORE,
					FeedVisitor.buildDicoTupleForStructure(this.directParentType, this.directParentId, this.dictionaryId,joinStoreFormat)
					);
		}
		FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);

		FeedVisitor.add(stat, this.transaction, DatastoreConstants.DICTIONARY_STORE, tuple);

		final IStatisticAttribute fieldAttr = stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_FIELD);
		if (fieldAttr != null) {
			this.field = fieldAttr.asText();
		}

		final ParentType previousParentType = this.directParentType;
		final String previousParentId = this.directParentId;
		this.directParentType = ParentType.DICTIONARY;
		this.directParentId = String.valueOf(this.dictionaryId);
		recordFieldForStructure(this.directParentType, this.directParentId);
		recordIndexForStructure(this.directParentType, this.directParentId);
		recordRefForStructure(this.directParentType, this.directParentId);

		visitChildren(stat);
		this.directParentType = previousParentType;
		this.directParentId = previousParentId;

		// Reset
		this.dictionaryId = null;
		this.field = null;
		return null;
	}

	protected void processDictionaryManager(final IMemoryStatistic stat) {
		this.rootComponent = ParentType.DICTIONARY;
		visitChildren(stat);
		this.rootComponent = null;
	}

	protected void processStoreStat(final IMemoryStatistic stat) {
		final IStatisticAttribute nameAttr = Objects.requireNonNull(
				stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_STORE_NAME),
				() -> "No store name in stat " + stat);
		this.store = nameAttr.asText();

		// Explore the store children
		visitChildren(stat);

		this.store = null;
	}

	protected void processStorePartition(final IMemoryStatistic stat) {
		final IStatisticAttribute partitionAttr = Objects.requireNonNull(
				stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_PARTITION_ID),
				() -> "No partition attribute in stat" + stat);
		this.partitionId = partitionAttr.asInt();

		visitChildren(stat);

		this.partitionId = null;
	}

	private void processRecords(final IMemoryStatistic stat) {
		final ParentType previousParentType = this.directParentType;
		final String previousParentId = this.directParentId;

		this.directParentType = ParentType.RECORDS;
		this.directParentId = String
				.valueOf(stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_RECORD_SET_ID).asLong());
		this.rootComponent = ParentType.RECORDS;
		visitChildren(stat);
		this.directParentType = previousParentType;
		this.directParentId = previousParentId;
		this.rootComponent = null;
	}

	private void processRecordVersion(final IMemoryStatistic stat) {
		isVersionColumn = MemoryStatisticConstants.STAT_NAME_VERSIONS_COLUMN.equals(stat.getName());

		visitChildren(stat);
		isVersionColumn = false;

	}

	private void processPrimaryIndices(final IMemoryStatistic stat) {
		processIndexList(stat, IndexType.PRIMARY);
	}

	private void processSecondaryIndices(final IMemoryStatistic stat) {
		processIndexList(stat, IndexType.SECONDARY);
	}

	private void processIndexList(final IMemoryStatistic stat, final IndexType type) {
		this.indexType = type;
		visitChildren(stat);
		this.indexType = null;
	}

	private void recordStatAndExplore(final DefaultMemoryStatistic stat) {
		isVersionColumn = MemoryStatisticConstants.STAT_NAME_VERSIONS_COLUMN.equals(stat.getName());

		visitChildren(stat);

		isVersionColumn = false;
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

	private static Object[] buildIndexTupleFrom(final IRecordFormat format, final IndexStatistic stat) {
		final Object[] tuple = new Object[format.getFieldCount()];
		tuple[format.getFieldIndex(DatastoreConstants.INDEX_ID)] = stat
				.getAttribute(MemoryStatisticConstants.ATTR_NAME_INDEX_ID).asLong();

		final String[] fieldNames = stat.getAttribute(DatastoreConstants.FIELDS).asStringArray();
		assert fieldNames != null && fieldNames.length > 0 : "Cannot find fields in the attributes of " + stat;
		tuple[format.getFieldIndex(
				DatastoreConstants.INDEX__FIELDS)] = new MemoryAnalysisDatastoreDescription.StringArrayObject(
						fieldNames);

		tuple[format.getFieldIndex(DatastoreConstants.INDEX_CLASS)] = stat.getAttribute(DatastoreConstants.INDEX_CLASS)
				.asText();

		return tuple;
	}

	/**
	 * Build an object array that represents a reference.
	 *
	 * @param referenceStatistic the {@link ReferenceStatistic} from which the array
	 *                           must be built.
	 * @return the object array.
	 */
	protected Object[] buildPartOfReferenceStatisticTuple(final IDatastoreSchemaMetadata storageMetadata,
			final ReferenceStatistic referenceStatistic) {
		final IRecordFormat refStoreFormat = getReferenceFormat(storageMetadata);

		final Object[] tuple = new Object[refStoreFormat.getFieldCount()];
		tuple[refStoreFormat.getFieldIndex(DatastoreConstants.REFERENCE_FROM_STORE)] = referenceStatistic
				.getAttribute(DatastoreConstants.REFERENCE_FROM_STORE).asText();
		tuple[refStoreFormat.getFieldIndex(DatastoreConstants.REFERENCE_FROM_STORE_PARTITION_ID)] = referenceStatistic
				.getAttribute(DatastoreConstants.REFERENCE_FROM_STORE_PARTITION_ID).asInt();
		tuple[refStoreFormat.getFieldIndex(DatastoreConstants.REFERENCE_TO_STORE)] = referenceStatistic
				.getAttribute(DatastoreConstants.REFERENCE_TO_STORE).asText();
		tuple[refStoreFormat.getFieldIndex(DatastoreConstants.REFERENCE_TO_STORE_PARTITION_ID)] = referenceStatistic
				.getAttribute(DatastoreConstants.REFERENCE_TO_STORE_PARTITION_ID).asInt();

		return tuple;
	}

}
