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

    if (currentOwner == this.sharedOwnerValue
        && currentComponent == this.sharedComponentValue
        && currentPartition == this.sharedPartitionId) {
      // We cannot make any change
      return previousRecord;
    } else {
      final int newOwner = getDicOwner(duplicateRecord);
      final int newComponent = getDicComponent(duplicateRecord);
      final int newPartition = getPartition(duplicateRecord);

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

        return newRecord;
      }
    }
  }

  private void init(
      final IStoreMetadata storeMetadata, final Records.IDictionaryProvider dictionaryProvider) {
    if (sharedComponentValue < 0) {
      final int ownerIdx = storeMetadata.getFieldIndex(DatastoreConstants.CHUNK__OWNER);
      @SuppressWarnings("unchecked")
      final IWritableDictionary<Object> ownerDictionary =
          (IWritableDictionary<Object>) dictionaryProvider.getDictionary(ownerIdx);
      sharedOwnerValue = ownerDictionary.map(MemoryAnalysisDatastoreDescription.SHARED_OWNER);

      final int componentIdx = storeMetadata.getFieldIndex(DatastoreConstants.CHUNK__COMPONENT);
      @SuppressWarnings("unchecked")
      final IWritableDictionary<Object> componentDictionary =
          (IWritableDictionary<Object>) dictionaryProvider.getDictionary(componentIdx);
      sharedComponentValue =
          componentDictionary.map(MemoryAnalysisDatastoreDescription.SHARED_COMPONENT);
    }
    if (sharedPartitionId < 0) {
      final int partitionIdx = storeMetadata.getFieldIndex(DatastoreConstants.CHUNK__PARTITION_ID);
      @SuppressWarnings("unchecked")
      final IWritableDictionary<Object> partitionDictionary =
          (IWritableDictionary<Object>) dictionaryProvider.getDictionary(partitionIdx);
      sharedPartitionId =
          partitionDictionary.map(MemoryAnalysisDatastoreDescription.MANY_PARTITIONS);
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

  private IWritableRecord copyRecord(final IRecordReader record) {
    final IRecordFormat format = record.getFormat();
    final IWritableRecord newRecord = format.newRecord();
    for (int i = 0, end_ = format.getFieldCount(); i < end_; i += 1) {
      newRecord.write(i, record.read(i));
    }

    return newRecord;
  }
}
