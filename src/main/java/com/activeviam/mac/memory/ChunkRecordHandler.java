package com.activeviam.mac.memory;

import com.qfs.desc.IDuplicateKeyHandler;
import com.qfs.dic.IDictionary;
import com.qfs.dic.IWritableDictionary;
import com.qfs.store.IStoreMetadata;
import com.qfs.store.record.IRecordFormat;
import com.qfs.store.record.IRecordReader;
import com.qfs.store.record.IWritableRecord;
import com.qfs.store.record.impl.Records;

public class ChunkRecordHandler implements IDuplicateKeyHandler {

	static int sharedOwnerValue = -1;
	static int sharedComponentValue = -1;

	@Override
	public IRecordReader selectDuplicateKeyWithinTransaction(
			final IRecordReader duplicateRecord,
			final IRecordReader previousRecord,
			final IStoreMetadata storeMetadata,
			final Records.IDictionaryProvider dictionaryProvider,
			final int[] primaryIndexFields,
			final int partitionId) {
		init(storeMetadata, dictionaryProvider);
		final int ownerIdx = storeMetadata.getFieldIndex(DatastoreConstants.CHUNK__OWNER);

		if (previousRecord.readInt(ownerIdx) == sharedOwnerValue) {
			return previousRecord;
		} else {
			final String currentOwner = getOwner(previousRecord, dictionaryProvider);
			final String newOwner = getOwner(duplicateRecord, dictionaryProvider);
			if (newOwner.equals(currentOwner)) {
				return duplicateRecord;
			} else {
				final IWritableRecord newRecord = copyRecord(duplicateRecord);
				// Change the owner to the special value "shared"
				newRecord.writeInt(ownerIdx, sharedOwnerValue);

				return newRecord;
			}
		}
	}

	@Override
	public IRecordReader selectDuplicateKeyInDatastore(
			final IRecordReader duplicateRecord,
			final IRecordReader previousRecord,
			final IStoreMetadata storeMetadata,
			final Records.IDictionaryProvider dictionaryProvider,
			final int[] primaryIndexFields,
			final int partitionId) {
		// TODO(ope) complete the error
		throw new IllegalStateException(
				"Cannot override an existing record. Consider deleting first the records by dumpName before inserting new ones");
	}

	private void init(
			final IStoreMetadata storeMetadata,
			final Records.IDictionaryProvider dictionaryProvider) {
		if (sharedComponentValue < 0) {
			final int ownerIdx = storeMetadata.getFieldIndex(DatastoreConstants.CHUNK__OWNER);
			@SuppressWarnings("unchecked")
			final IWritableDictionary<Object> ownerDictionary = (IWritableDictionary<Object>) dictionaryProvider.getDictionary(ownerIdx);
			sharedOwnerValue = ownerDictionary.map(MemoryAnalysisDatastoreDescription.SHARED_OWNER);

			final int componentIdx = storeMetadata.getFieldIndex(DatastoreConstants.CHUNK__COMPONENT);
			@SuppressWarnings("unchecked")
			final IWritableDictionary<Object> componentDictionary = (IWritableDictionary<Object>) dictionaryProvider.getDictionary(componentIdx);
			sharedComponentValue = componentDictionary.map(MemoryAnalysisDatastoreDescription.SHARED_COMPONENT);
		}
	}

	private String getOwner(final IRecordReader record, final Records.IDictionaryProvider dictionaryProvider) {
		final int idx = record.getFormat().getFieldIndex(DatastoreConstants.CHUNK__OWNER);
		final int owner = record.readInt(idx);
		final IDictionary<?> dictionary = dictionaryProvider.getDictionary(idx);
		return (String) dictionary.read(owner);
	}

	private int getDicOwner(final IRecordReader record) {
		final int idx = record.getFormat().getFieldIndex(DatastoreConstants.CHUNK__OWNER);
		return record.readInt(idx);
	}

	private IWritableRecord copyRecord(final IRecordReader record) {
		final IRecordFormat format = record.getFormat();
		final IWritableRecord newRecord = format.newRecord();
		for (int i = 0, end_ = format.getFieldCount(); i < end_; i += 1) {
			newRecord.write(i, record.read(i));
		}

		return newRecord;
	}

}
