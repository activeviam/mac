/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.statistic.memory.visitor.impl;

import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription;
import com.qfs.monitoring.statistic.IStatisticAttribute;
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
import com.quartetfs.biz.pivot.definitions.impl.Field;

import java.time.Instant;

import static com.activeviam.mac.statistic.memory.visitor.impl.FeedVisitor.visitChildren;

public class ChunkSetStatisticVisitor implements IMemoryStatisticVisitor<Void> {

	protected final IDatastoreSchemaMetadata storageMetadata;
	protected final IOpenedTransaction transaction;
	/** The record format of the store that stores the chunks. */
	protected final IRecordFormat chunkRecordFormat;
	/** The record format of {@link DatastoreConstants#CHUNK_AND_STORE__STORE_NAME} */
	protected final IRecordFormat chunkAndStoreRecordFormat;
	/** The name of the off-heap dump. Can be null */
	protected final String dumpName;
	/** The export date, found on the first statistics we read */
	protected final Instant current;
	/** The name of the store of the visited statistic */
	protected final String store;
	/** The partition id of the visited statistic */
	protected final int partitionId;

	/** ID of the current {@link ChunkSet}. */
	protected Long chunkSetId = null;
	protected String field = null;
	protected boolean visitingRowMapping = false;
	protected boolean visitingVectorBlock = false;

	public ChunkSetStatisticVisitor(
			final IDatastoreSchemaMetadata storageMetadata,
			final IOpenedTransaction transaction,
			final String dumpName,
			final Instant current,
			final String store,
			final int partitionId) {
		this.storageMetadata = storageMetadata;
		this.transaction = transaction;
		this.dumpName = dumpName;
		this.current = current;
		this.store = store;
		this.partitionId = partitionId;
		this.chunkRecordFormat = this.storageMetadata
				.getStoreMetadata(DatastoreConstants.CHUNK_STORE)
				.getStoreFormat()
				.getRecordFormat();
		this.chunkAndStoreRecordFormat = this.storageMetadata
				.getStoreMetadata(DatastoreConstants.CHUNK_AND_STORE__STORE_NAME)
				.getStoreFormat()
				.getRecordFormat();
	}

	@Override
	public Void visit(DefaultMemoryStatistic memoryStatistic) {
		if (memoryStatistic.getName().equals(MemoryStatisticConstants.ATTR_NAME_ROW_MAPPING)) {
			this.visitingRowMapping = true;
		} else if (memoryStatistic.getName().equals(MemoryStatisticConstants.STAT_NAME_CHUNK_ENTRY)) {
			this.visitingRowMapping = false;
		} else {
			throw new RuntimeException("unexpected statistic " + memoryStatistic);
		}

		FeedVisitor.visitChildren(this, memoryStatistic);

		this.visitingRowMapping = false;

		return null;
	}

	@Override
	public Void visit(ChunkSetStatistic chunkSetStatistic) {
		final IRecordFormat format = FeedVisitor.getChunksetFormat(this.storageMetadata);
		final Object[] tuple = FeedVisitor.buildChunksetTupleFrom(format, chunkSetStatistic);

		FeedVisitor.add(chunkSetStatistic, this.transaction, DatastoreConstants.CHUNKSET_STORE, tuple);

		this.chunkSetId = (Long) tuple[format.getFieldIndex(DatastoreConstants.CHUNKSET_ID)];

		FeedVisitor.visitChildren(this, chunkSetStatistic);

		// Reset
		this.chunkSetId = null;

		return null;
	}

	@Override
	public Void visit(ChunkStatistic chunkStatistic) {
		if (chunkStatistic.getName().equals(MemoryStatisticConstants.STAT_NAME_CHUNK_OF_CHUNKSET)) {
			final IStatisticAttribute fieldAttribute = chunkStatistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_FIELD);
			this.field = fieldAttribute.asText();
		} else if (chunkStatistic.getName().equals(MemoryStatisticConstants.STAT_NAME_VECTOR_BLOCK)) {
			this.visitingVectorBlock = true;
		}
		final Object[] tuple = FeedVisitor.buildChunkTupleFrom(this.chunkRecordFormat, chunkStatistic);

		// FIXME(ope) this is wrong, we should really read the origin of the chunkset
		FeedVisitor.setTupleElement(tuple, chunkRecordFormat, DatastoreConstants.CHUNK__PARENT_TYPE, MemoryAnalysisDatastoreDescription.ParentType.RECORDS);
		FeedVisitor.setTupleElement(tuple, chunkRecordFormat, DatastoreConstants.CHUNK__PARENT_ID, String.valueOf(this.chunkSetId));

//		FeedVisitor.setTupleElement(tuple, chunkRecordFormat, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);
//		FeedVisitor.setTupleElement(tuple, chunkRecordFormat, DatastoreConstants.CHUNK__EXPORT_DATE, this.current);
//		FeedVisitor.setTupleElement(tuple, chunkRecordFormat, DatastoreConstants.CHUNKSET_ID, this.chunkSetId);
//		FeedVisitor.setTupleElement(tuple, chunkRecordFormat, DatastoreConstants.CHUNK__TYPE, FeedVisitor.TYPE_RECORD);
//		FeedVisitor.setTupleElement(tuple, chunkRecordFormat, DatastoreConstants.CHUNK__PARTITION_ID, this.partitionId);

		if (!this.visitingRowMapping && !this.visitingVectorBlock) {
			// A block of vector does not belong to a single field
			FeedVisitor.setTupleElement(tuple, chunkRecordFormat, DatastoreConstants.CHUNK__FIELD, this.field);
		}

		// Debug
		tuple[chunkRecordFormat.getFieldIndex(DatastoreConstants.CHUNK__DEBUG_TREE)] = StatisticTreePrinter.getTreeAsString(chunkStatistic);

		FeedVisitor.add(chunkStatistic, this.transaction, DatastoreConstants.CHUNK_STORE, tuple);

		final Object[] tupleChunkAndStore = FeedVisitor.buildChunkAndStoreTuple(this.chunkAndStoreRecordFormat, chunkStatistic, this.store);
		FeedVisitor.add(chunkStatistic, this.transaction, DatastoreConstants.CHUNK_AND_STORE__STORE_NAME, tupleChunkAndStore);

		visitChildren(this, chunkStatistic);

		this.field = null;
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
}
