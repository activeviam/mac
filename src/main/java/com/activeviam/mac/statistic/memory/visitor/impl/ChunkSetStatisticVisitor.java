/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.statistic.memory.visitor.impl;

import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription.ParentType;
import com.qfs.monitoring.statistic.IStatisticAttribute;
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

import java.time.Instant;

public class ChunkSetStatisticVisitor extends AFeedVisitor<Void> {

	/** The record format of the store that stores the chunks. */
	protected final IRecordFormat chunkRecordFormat;
	/** The record format of {@link DatastoreConstants#CHUNK_TO_FIELD_STORE} */

	protected final IRecordFormat chunkToFieldFormat;

	/** The export date, found on the first statistics we read */
	protected final Instant current;
	/** The name of the store of the visited statistic */
	protected final String store;
	private final ParentType rootComponent;
	private final ParentType directParentType;
	private final String directParentId;
	/** The partition id of the visited statistic */
	protected final int partitionId;

	/** ID of the current {@link ChunkSet}. */
	protected Long chunkSetId = null;
	protected String field = null;
	protected boolean visitingRowMapping = false;
	protected boolean visitingVectorBlock = false;

	protected Integer chunkSize;
	protected Integer freeRows;
	protected Integer nonWrittenRows;

	private final Long referenceId;
	private final Long indexId;

	public ChunkSetStatisticVisitor(
			final IDatastoreSchemaMetadata storageMetadata,
			final IOpenedTransaction transaction,
			final String dumpName,
			final Instant current,
			final String store,
			final ParentType rootComponent,
			final ParentType parentType,
			final String parentId,
			final int partitionId,
			final Long indexId,
			final Long referenceId) {
		super(transaction, storageMetadata, dumpName);
		this.current = current;
		this.store = store;
		this.rootComponent = rootComponent;
		this.directParentType = parentType;
		this.directParentId = parentId;
		this.partitionId = partitionId;
		this.indexId = indexId;
		this.referenceId = referenceId;

		this.chunkRecordFormat = this.storageMetadata
				.getStoreMetadata(DatastoreConstants.CHUNK_STORE)
				.getStoreFormat()
				.getRecordFormat();
		this.chunkToFieldFormat = this.storageMetadata
				.getStoreMetadata(DatastoreConstants.CHUNK_TO_FIELD_STORE)
				.getStoreFormat()
				.getRecordFormat();
	}

	@Override
	public Void visit(DefaultMemoryStatistic memoryStatistic) {
		if (memoryStatistic.getName().equals(MemoryStatisticConstants.STAT_NAME_ROW_MAPPING)) {
			this.visitingRowMapping = true;

			FeedVisitor.visitChildren(this, memoryStatistic);

			this.visitingRowMapping = false;
		} else if (memoryStatistic.getName().equals(MemoryStatisticConstants.STAT_NAME_CHUNK_ENTRY)) {
			this.visitingRowMapping = false;

			// Remove this stat for a subchunk, particularly for vector chunks
			final Integer previousSize = this.chunkSize;
			final Integer previousFree = this.freeRows;
			final Integer previousNonWritten = this.nonWrittenRows;
			this.chunkSize = null;
			this.freeRows = null;
			this.nonWrittenRows = null;

			FeedVisitor.visitChildren(this, memoryStatistic);

			this.chunkSize = previousSize;
			this.freeRows = previousFree;
			this.nonWrittenRows = previousNonWritten;
		} else {
			throw new RuntimeException("unexpected statistic " + memoryStatistic);
		}

		return null;
	}

	@Override
	public Void visit(final ChunkSetStatistic statistic) {
		recordFieldForStructure(this.directParentType, this.directParentId);

		this.chunkSize = statistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_LENGTH).asInt();
		this.freeRows = statistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_FREED_ROWS).asInt();
		this.nonWrittenRows = statistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_NOT_WRITTEN_ROWS).asInt();
		this.chunkSetId = statistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_CHUNKSET_ID).asLong();

		FeedVisitor.visitChildren(this, statistic);

		// Reset
		this.chunkSetId = null;
		this.chunkSize = null;
		this.freeRows = null;
		this.nonWrittenRows = null;

		return null;
	}

	@Override
	public Void visit(final ChunkStatistic chunkStatistic) {
		recordIndexForStructure(this.directParentType, this.directParentId);
		recordRefForStructure(this.directParentType, this.directParentId);

		final String previousField = this.field;
		if (chunkStatistic.getName().equals(MemoryStatisticConstants.STAT_NAME_CHUNK_OF_CHUNKSET)) {
			final IStatisticAttribute fieldAttribute = chunkStatistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_FIELD);
			this.field = fieldAttribute.asText();

			recordFieldForStructure(this.directParentType, this.directParentId);
		} else if (chunkStatistic.getName().equals(MemoryStatisticConstants.STAT_NAME_VECTOR_BLOCK)) {
			this.visitingVectorBlock = true;
			// TODO(ope) Somehow invert the logic for to be able to assign a vector to its parent block
			// Currently, vectors are first discovered as part of a ChunkObject and then the vector block is visited
		}

		final IRecordFormat format = this.chunkRecordFormat;
		final Object[] tuple = FeedVisitor.buildChunkTupleFrom(format, chunkStatistic);

		FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__PARENT_TYPE, this.directParentType);
		FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__PARENT_ID, this.directParentId);

		FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);

		FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__OWNER, this.store);
		FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__COMPONENT, this.rootComponent.toString());
		FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__PARTITION_ID, this.partitionId);

		// Complete chunk info regarding size and usage if not defined by a parent
		if (!this.visitingVectorBlock) {
			if (this.chunkSize != null) {
				FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__SIZE, this.chunkSetId);
			}
			if (this.freeRows != null) {
				FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__FREE_ROWS, this.freeRows);
			}
			if (this.nonWrittenRows != null) {
				FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__NON_WRITTEN_ROWS, this.nonWrittenRows);
			}
		}

		// Debug
		tuple[format.getFieldIndex(DatastoreConstants.CHUNK__DEBUG_TREE)] = StatisticTreePrinter.getTreeAsString(chunkStatistic);

		FeedVisitor.add(chunkStatistic, this.transaction, DatastoreConstants.CHUNK_STORE, tuple);

		visitChildren(chunkStatistic);

		this.field = previousField;
		this.visitingVectorBlock = false;

		return null;
	}

	// NOT RELEVANT

	@Override
	public Void visit(ReferenceStatistic referenceStatistic) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Void visit(IndexStatistic indexStatistic) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Void visit(DictionaryStatistic dictionaryStatistic) {
		throw new UnsupportedOperationException();
	}

	private void recordFieldForStructure(
			final ParentType type,
			final String id) {
		// FIXME(ope) duplicated from com.activeviam.mac.statistic.memory.visitor.impl.DatastoreFeederVisitor
		if (this.store != null && this.field != null) {
			final IRecordFormat format = this.chunkToFieldFormat;
			final Object[] tuple = new Object[format.getFieldCount()];
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK_TO_FIELD__STORE, this.store);
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK_TO_FIELD__FIELD, this.field);
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK_TO_FIELD__PARENT_TYPE, type);
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK_TO_FIELD__PARENT_ID, id);

			this.transaction.add(DatastoreConstants.CHUNK_TO_FIELD_STORE, tuple);
		}
	}

	private void recordIndexForStructure(
			final ParentType type,
			final String id) {
		// FIXME(ope) duplicated from com.activeviam.mac.statistic.memory.visitor.impl.DatastoreFeederVisitor
		if (this.indexId != null) {
			final IRecordFormat format = FeedVisitor.getRecordFormat(this.storageMetadata, DatastoreConstants.CHUNK_TO_INDEX_STORE);
			final Object[] tuple = new Object[format.getFieldCount()];
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK_TO_INDEX__INDEX_ID, this.indexId);
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK_TO_INDEX__PARENT_TYPE, type);
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK_TO_INDEX__PARENT_ID, id);

			this.transaction.add(DatastoreConstants.CHUNK_TO_INDEX_STORE, tuple);
		}
	}

	private void recordRefForStructure(
			final ParentType type,
			final String id) {
		// FIXME(ope) duplicated from com.activeviam.mac.statistic.memory.visitor.impl.DatastoreFeederVisitor
		if (this.referenceId != null) {
			final IRecordFormat format = FeedVisitor.getRecordFormat(this.storageMetadata, DatastoreConstants.CHUNK_TO_REF_STORE);
			final Object[] tuple = new Object[format.getFieldCount()];
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK_TO_REF__REF_ID, this.referenceId);
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK_TO_REF__PARENT_TYPE, type);
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK_TO_REF__PARENT_ID, id);

			this.transaction.add(DatastoreConstants.CHUNK_TO_REF_STORE, tuple);
		}
	}

}
