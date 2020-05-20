/*
 * (C) ActiveViam 2007-2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.memory;

import com.qfs.desc.IDuplicateKeyHandler;
import com.qfs.dic.IWritableDictionary;
import com.qfs.store.IStoreMetadata;
import com.qfs.store.record.IRecordFormat;
import com.qfs.store.record.IRecordReader;
import com.qfs.store.record.IWritableRecord;
import com.qfs.store.record.impl.Records;
import com.qfs.store.record.impl.Records.IDictionaryProvider;
import java.util.Arrays;

/**
 * {@link IDuplicateKeyHandler} implementation defining the process of dealing with duplicated
 * entries of the same Chunk in the Memory Analysis Cube application.
 *
 * @author ActiveViam
 */
public class ChunkRecordHandler implements IDuplicateKeyHandler {

	private int sharedOwnerValue = -1;
	private int sharedComponentValue = -1;
	private int sharedPartitionId = MemoryAnalysisDatastoreDescription.MANY_PARTITIONS;
	private int sharedStoreId = -1;
	private int sharedFieldId = -1;
	private int unknownStoreId = -1;
	private int unknownFieldId = -1;
	private int defaultDicId = -1;
	private int defaultRefId = -1;
	private int defaultIdxId = -1;


	@Override
	public IRecordReader selectDuplicateKeyWithinTransaction(
			final IRecordReader duplicateRecord,
			final IRecordReader previousRecord,
			final IStoreMetadata storeMetadata,
			final Records.IDictionaryProvider dictionaryProvider,
			final int[] primaryIndexFields,
			final int partitionId) {
		return createMergedRecord(duplicateRecord, previousRecord, storeMetadata, dictionaryProvider);
	}

	@Override
	public IRecordReader selectDuplicateKeyInDatastore(
			final IRecordReader duplicateRecord,
			final IRecordReader previousRecord,
			final IStoreMetadata storeMetadata,
			final Records.IDictionaryProvider dictionaryProvider,
			final int[] primaryIndexFields,
			final int partitionId) {
		return createMergedRecord(duplicateRecord, previousRecord, storeMetadata, dictionaryProvider);
	}

	private IRecordReader createMergedRecord(
			IRecordReader duplicateRecord,
			IRecordReader previousRecord,
			IStoreMetadata storeMetadata,
			IDictionaryProvider dictionaryProvider) {
		init(storeMetadata, dictionaryProvider);

		final int currentOwner = getDicOwner(previousRecord);
		final int currentComponent = getDicComponent(previousRecord);
		final int currentPartition = getPartition(previousRecord);
		final int currentStore = getStore(previousRecord);
		final int currentField = getField(previousRecord);
		final long currentDicId = getDicId(previousRecord);
		final long currentRefId = getRefId(previousRecord);
		final long currentIdxId = getIdxId(previousRecord);

		if (currentOwner == this.sharedOwnerValue
				&& currentComponent == this.sharedComponentValue
				&& currentPartition == this.sharedPartitionId) {
			// We cannot make any change
			return previousRecord;
		} else {
			final int newOwner = getDicOwner(duplicateRecord);
			final int newComponent = getDicComponent(duplicateRecord);
			final int newPartition = getPartition(duplicateRecord);
			final int newStore = getStore(duplicateRecord);
			final int newField = getField(duplicateRecord);
			final long newDicId = getDicId(duplicateRecord);
			final long newRefId = getRefId(duplicateRecord);
			final long newIdxId = getIdxId(duplicateRecord);

			if (newOwner == currentOwner
					&& newComponent == currentComponent
					&& newPartition == currentPartition) {
				// Nothing to change
				return duplicateRecord;
			} else {
				final IWritableRecord newRecord = copyRecord(duplicateRecord);

				if (newOwner != currentOwner) {
					// Change the owner to the special value "shared"
					final int ownerIdx = storeMetadata.getFieldIndex(DatastoreConstants.CHUNK__OWNER);
					newRecord.writeInt(ownerIdx, this.sharedOwnerValue);
				}
				if (newComponent != currentComponent) {
					final int componentIdx = storeMetadata.getFieldIndex(DatastoreConstants.CHUNK__COMPONENT);
					newRecord.writeInt(componentIdx, this.sharedComponentValue);
				}
				if (newPartition != currentPartition) {
					assert newPartition != MemoryAnalysisDatastoreDescription.NO_PARTITION;
					assert currentPartition != MemoryAnalysisDatastoreDescription.NO_PARTITION;
					final int partitionIdx =
							storeMetadata.getFieldIndex(DatastoreConstants.CHUNK__PARTITION_ID);
					newRecord.writeInt(partitionIdx, this.sharedPartitionId);
				}
				if (newStore != currentStore) {
					// If the new record has no store knowledge, use the previous instead
					final int storeIdx = storeMetadata
												.getFieldIndex(DatastoreConstants.CHUNK__PARENT_STORE_NAME);
					if(newStore == unknownStoreId){
						newRecord.write(storeIdx, currentStore);
					} else {
						newRecord.write(storeIdx, sharedStoreId);
					}
				}
				if (newField != currentField) {
					final int fieldIndex = storeMetadata
							.getFieldIndex(DatastoreConstants.CHUNK__PARENT_FIELD_NAME);
					if(newField == unknownFieldId){
						newRecord.write(fieldIndex, currentField);
					} else {
						newRecord.write(fieldIndex, sharedFieldId);
					}
				}
				// If component-specific ids are both not the default value and do not match, we throw
				// else we keep the non-default
				if (currentDicId != newDicId) {
					if (currentDicId != defaultDicId && newDicId != defaultDicId) {
						throw new IllegalStateException("Cannot merge a chunk record coming from two different "
								+ "dictionaries. Something went wrong");
					}
					final int dicIdIdx = storeMetadata
							.getFieldIndex(DatastoreConstants.CHUNK__PARENT_DICO_ID);
					newRecord.write(dicIdIdx, currentDicId == defaultDicId ? newDicId : currentDicId);
				}
				if (currentRefId != newRefId) {
					if (currentRefId != defaultRefId && newRefId != defaultRefId) {
						throw new IllegalStateException("Cannot merge a chunk record coming from two different "
								+ "references. Something went wrong");
					}
					final int refIdIdx = storeMetadata
							.getFieldIndex(DatastoreConstants.CHUNK__PARENT_REF_ID);
					newRecord.write(refIdIdx, currentRefId == defaultRefId ? newRefId : currentRefId);
				}
				if (currentIdxId != newIdxId) {
					if (currentIdxId != defaultIdxId && newIdxId != defaultIdxId) {
						throw new IllegalStateException("Cannot merge a chunk record coming from two different "
								+ "indexes. Something went wrong");
					}
					final int IdxIdIdx = storeMetadata
							.getFieldIndex(DatastoreConstants.CHUNK__PARENT_INDEX_ID);
					newRecord.write(IdxIdIdx, currentIdxId == defaultIdxId ? newIdxId : currentIdxId);
				}
				return newRecord;
			}
		}
	}

	private void init(
			final IStoreMetadata storeMetadata, final Records.IDictionaryProvider dictionaryProvider) {
		if (sharedComponentValue < 0) {
			final int ownerIdx = storeMetadata.getFieldIndex(DatastoreConstants.CHUNK__OWNER);
			@SuppressWarnings("unchecked") final IWritableDictionary<Object> ownerDictionary =
					(IWritableDictionary<Object>) dictionaryProvider.getDictionary(ownerIdx);
			sharedOwnerValue = ownerDictionary.map(MemoryAnalysisDatastoreDescription.SHARED_OWNER);

			final int componentIdx = storeMetadata.getFieldIndex(DatastoreConstants.CHUNK__COMPONENT);
			@SuppressWarnings("unchecked") final IWritableDictionary<Object> componentDictionary =
					(IWritableDictionary<Object>) dictionaryProvider.getDictionary(componentIdx);
			sharedComponentValue =
					componentDictionary.map(MemoryAnalysisDatastoreDescription.SHARED_COMPONENT);
		}
		if (sharedPartitionId < 0) {
			final int partitionIdx = storeMetadata.getFieldIndex(DatastoreConstants.CHUNK__PARTITION_ID);
			@SuppressWarnings("unchecked") final IWritableDictionary<Object> partitionDictionary =
					(IWritableDictionary<Object>) dictionaryProvider.getDictionary(partitionIdx);
			sharedPartitionId =
					partitionDictionary.map(MemoryAnalysisDatastoreDescription.MANY_PARTITIONS);
		}

		if (sharedStoreId < 0) {
			final int storeIdx = storeMetadata.getFieldIndex(DatastoreConstants.CHUNK__PARENT_STORE_NAME);
			@SuppressWarnings("unchecked") final IWritableDictionary<Object> storeDictionary =
					(IWritableDictionary<Object>) dictionaryProvider.getDictionary(storeIdx);
			sharedStoreId = storeDictionary
					.map(MemoryAnalysisDatastoreDescription.DATASTORE_SHARED);
		}
		if (sharedFieldId < 0) {
			final int fieldIdx = storeMetadata.getFieldIndex(DatastoreConstants.CHUNK__PARENT_FIELD_NAME);
			@SuppressWarnings("unchecked") final IWritableDictionary<Object> fieldDictionary =
					(IWritableDictionary<Object>) dictionaryProvider.getDictionary(fieldIdx);
			sharedFieldId = fieldDictionary
					.map(MemoryAnalysisDatastoreDescription.DATASTORE_SHARED);
		}
		if (unknownStoreId < 0) {
			final int storeIdx = storeMetadata.getFieldIndex(DatastoreConstants.CHUNK__PARENT_STORE_NAME);
			@SuppressWarnings("unchecked") final IWritableDictionary<Object> storeDictionary =
					(IWritableDictionary<Object>) dictionaryProvider.getDictionary(storeIdx);
			unknownStoreId = storeDictionary
					.map(MemoryAnalysisDatastoreDescription.DEFAULT_DATASTORE);
		}
		if (unknownFieldId < 0) {
			final int fieldIdx = storeMetadata.getFieldIndex(DatastoreConstants.CHUNK__PARENT_FIELD_NAME);
			@SuppressWarnings("unchecked") final IWritableDictionary<Object> fieldDictionary =
					(IWritableDictionary<Object>) dictionaryProvider.getDictionary(fieldIdx);
			unknownFieldId = fieldDictionary
					.map(MemoryAnalysisDatastoreDescription.DEFAULT_DATASTORE);
		}
		if (defaultDicId < 0) {
			final int dicIdIdx = storeMetadata
					.getFieldIndex(DatastoreConstants.CHUNK__PARENT_DICO_ID);
			@SuppressWarnings("unchecked") final IWritableDictionary<Object> dicIdDictionary =
					(IWritableDictionary<Object>) dictionaryProvider.getDictionary(dicIdIdx);
			defaultDicId = dicIdDictionary
					.map(MemoryAnalysisDatastoreDescription.DEFAULT_COMPONENT_ID_VALUE);
		}

		if (defaultIdxId < 0) {
			final int idxIdIdx = storeMetadata
					.getFieldIndex(DatastoreConstants.CHUNK__PARENT_INDEX_ID);
			@SuppressWarnings("unchecked") final IWritableDictionary<Object> idxIdDictionary =
					(IWritableDictionary<Object>) dictionaryProvider.getDictionary(idxIdIdx);
			defaultIdxId = idxIdDictionary
					.map(MemoryAnalysisDatastoreDescription.DEFAULT_COMPONENT_ID_VALUE);
		}

		if (defaultRefId < 0) {
			final int refIdIdx = storeMetadata
					.getFieldIndex(DatastoreConstants.CHUNK__PARENT_REF_ID);
			@SuppressWarnings("unchecked") final IWritableDictionary<Object> refIdDictionary =
					(IWritableDictionary<Object>) dictionaryProvider.getDictionary(refIdIdx);
			defaultRefId = refIdDictionary
					.map(MemoryAnalysisDatastoreDescription.DEFAULT_COMPONENT_ID_VALUE);
		}
	}

	private int getDicOwner(final IRecordReader record) {
		final int idx = record.getFormat().getFieldIndex(DatastoreConstants.CHUNK__OWNER);
		return record.readInt(idx);
	}

	private int getDicComponent(final IRecordReader record) {
		final int idx = record.getFormat().getFieldIndex(DatastoreConstants.CHUNK__COMPONENT);
		return record.readInt(idx);
	}

	private int getPartition(final IRecordReader record) {
		final int idx = record.getFormat().getFieldIndex(DatastoreConstants.CHUNK__PARTITION_ID);
		return record.readInt(idx);
	}

	private int getStore(final IRecordReader record) {
		final int idx = record.getFormat().getFieldIndex(DatastoreConstants.CHUNK__PARENT_STORE_NAME);
		return record.readInt(idx);
	}

	private int getField(final IRecordReader record) {
		final int idx = record.getFormat().getFieldIndex(DatastoreConstants.CHUNK__PARENT_FIELD_NAME);
		return record.readInt(idx);
	}

	private long getDicId(final IRecordReader record) {
		final int idx = record.getFormat().getFieldIndex(DatastoreConstants.CHUNK__PARENT_DICO_ID);
		return record.readLong(idx);
	}

	private long getIdxId(final IRecordReader record) {
		final int idx = record.getFormat().getFieldIndex(DatastoreConstants.CHUNK__PARENT_INDEX_ID);
		return record.readLong(idx);
	}

	private long getRefId(final IRecordReader record) {
		final int idx = record.getFormat().getFieldIndex(DatastoreConstants.CHUNK__PARENT_REF_ID);
		return record.readLong(idx);
	}

	private IWritableRecord copyRecord(final IRecordReader record) {
		final IRecordFormat format = record.getFormat();
		final IWritableRecord newRecord = format.newRecord();
		for (int i = 0, end = format.getFieldCount(); i < end; i += 1) {
			newRecord.write(i, record.read(i));
		}

		return newRecord;
	}
}
