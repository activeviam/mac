package com.activeviam.mac.statistic.memory.visitor.impl;

import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription.ParentType;
import com.qfs.store.IDatastoreSchemaMetadata;
import com.qfs.store.record.IRecordFormat;
import com.qfs.store.transaction.IOpenedTransaction;

public abstract class ADatastoreFeedVisitor<R> extends AFeedVisitor<R> {
	
	/** The name of the store of the visited statistic */
	protected String store = null;
	protected String field = null;

	protected Long indexId;
	protected Long referenceId;

	public ADatastoreFeedVisitor(IOpenedTransaction transaction, IDatastoreSchemaMetadata storageMetadata,
			String dumpName) {
		super(transaction, storageMetadata, dumpName);
	}

	protected void recordFieldForStructure(
			final ParentType type,
			final String id) {
		if (this.store != null && this.field != null) {
			final IRecordFormat format = FeedVisitor.getRecordFormat(this.storageMetadata, DatastoreConstants.CHUNK_TO_FIELD_STORE);
			final Object[] tuple = new Object[format.getFieldCount()];
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK_TO_FIELD__STORE, this.store);
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK_TO_FIELD__FIELD, this.field);
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK_TO_FIELD__PARENT_TYPE, type);
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK_TO_FIELD__PARENT_ID, id);

			this.transaction.add(DatastoreConstants.CHUNK_TO_FIELD_STORE, tuple);
		}
	}

	protected void recordIndexForStructure(
			final ParentType type,
			final String id) {
		if (this.indexId != null) {
			final IRecordFormat format = FeedVisitor.getRecordFormat(this.storageMetadata, DatastoreConstants.CHUNK_TO_INDEX_STORE);
			final Object[] tuple = new Object[format.getFieldCount()];
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK_TO_INDEX__INDEX_ID, this.indexId);
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK_TO_INDEX__PARENT_TYPE, type);
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK_TO_INDEX__PARENT_ID, id);

			this.transaction.add(DatastoreConstants.CHUNK_TO_INDEX_STORE, tuple);
		}
	}

	protected void recordRefForStructure(
			final ParentType type,
			final String id) {
		if (this.referenceId != null) {
			final IRecordFormat format = FeedVisitor.getRecordFormat(this.storageMetadata, DatastoreConstants.CHUNK_TO_REF_STORE);
			final Object[] tuple = new Object[format.getFieldCount()];
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK_TO_REF__REF_ID, this.referenceId);
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK_TO_REF__PARENT_TYPE, type);
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK_TO_REF__PARENT_ID, id);

			this.transaction.add(DatastoreConstants.CHUNK_TO_REF_STORE, tuple);
		}
	}
	protected void recordStructureParent(final ParentType type, final String id) {
		recordFieldForStructure(type, id);
		recordIndexForStructure(type, id);
		recordRefForStructure(type, id);
	}

}
