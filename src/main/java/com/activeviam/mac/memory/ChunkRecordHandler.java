/*
 * (C) ActiveViam 2007-2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.memory;

import com.qfs.chunk.impl.TombStoneChunk;
import com.qfs.desc.IDuplicateKeyHandler;
import com.qfs.dic.IWritableDictionary;
import com.qfs.store.IStoreMetadata;
import com.qfs.store.record.IRecordFormat;
import com.qfs.store.record.IRecordReader;
import com.qfs.store.record.IWritableRecord;
import com.qfs.store.record.impl.IDictionaryProvider;

/**
 * {@link IDuplicateKeyHandler} implementation defining the process of dealing with duplicated
 * entries of the same Chunk in the Memory Analysis Cube application.
 *
 * @author ActiveViam
 */
public class ChunkRecordHandler implements IDuplicateKeyHandler {

  private int sharedPartitionId = MemoryAnalysisDatastoreDescriptionConfig.MANY_PARTITIONS;
  private int defaultDicId = -1;
  private int defaultRefId = -1;
  private int defaultIdxId = -1;

  @Override
  public IRecordReader selectDuplicateKeyInDatastore(
      final IRecordReader duplicateRecord,
      final IRecordReader previousRecord,
      final IStoreMetadata storeMetadata,
      final IDictionaryProvider dictionaryProvider,
      final int[] uniqueIndexFields,
      final int partitionId) {
    return createMergedRecord(duplicateRecord, previousRecord, storeMetadata, dictionaryProvider);
  }

  private IRecordReader createMergedRecord(
      IRecordReader duplicateRecord,
      IRecordReader previousRecord,
      IStoreMetadata storeMetadata,
      IDictionaryProvider dictionaryProvider) {
    init(storeMetadata, dictionaryProvider);

    final int currentPartition = getPartition(previousRecord);
    final long currentDicId = getDicId(previousRecord);
    final long currentRefId = getRefId(previousRecord);
    final long currentIdxId = getIdxId(previousRecord);

    if (currentPartition == this.sharedPartitionId) {
      // We cannot make any change
      return previousRecord;
    } else {
      final int newPartition = getPartition(duplicateRecord);
      final long newDicId = getDicId(duplicateRecord);
      final long newRefId = getRefId(duplicateRecord);
      final long newIdxId = getIdxId(duplicateRecord);

      if (newPartition == currentPartition) {
        // Nothing to change
        return duplicateRecord;
      } else {
        // We ignore TombStoneChunks as they are a singleton that has minimal memory footprint
        // but don't work with the current MAC data model
        if (getChunkClassName(duplicateRecord, dictionaryProvider, storeMetadata)
            .contains(TombStoneChunk.class.getName())) {
          return duplicateRecord;
        }
        final IWritableRecord newRecord = copyRecord(duplicateRecord);

        assert newPartition != MemoryAnalysisDatastoreDescriptionConfig.NO_PARTITION;
        assert currentPartition != MemoryAnalysisDatastoreDescriptionConfig.NO_PARTITION;
        final int partitionIdx =
            storeMetadata.getFieldIndex(DatastoreConstants.CHUNK__PARTITION_ID);
        newRecord.writeInt(partitionIdx, this.sharedPartitionId);

        // Sanity check in case two Chunks have different parents which should never happen
        if (currentDicId != newDicId) {
          throw new IllegalStateException(
              "Cannot merge a chunk record coming from two different "
                  + "dictionaries. Something went wrong");
        }
        if (currentRefId != newRefId) {
          throw new IllegalStateException(
              "Cannot merge a chunk record coming from two different "
                  + "references. Something went wrong");
        }
        if (currentIdxId != newIdxId) {
          throw new IllegalStateException(
              "Cannot merge a chunk record coming from two different "
                  + "indexes. Something went wrong");
        }
        return newRecord;
      }
    }
  }

  private void init(
      final IStoreMetadata storeMetadata, final IDictionaryProvider dictionaryProvider) {
    if (this.sharedPartitionId < 0) {
      final int partitionIdx = storeMetadata.getFieldIndex(DatastoreConstants.CHUNK__PARTITION_ID);
      @SuppressWarnings("unchecked")
      final IWritableDictionary<Object> partitionDictionary =
          (IWritableDictionary<Object>) dictionaryProvider.getDictionary(partitionIdx);
      this.sharedPartitionId =
          partitionDictionary.map(MemoryAnalysisDatastoreDescriptionConfig.MANY_PARTITIONS);
    }
    if (this.defaultDicId < 0) {
      final int dicIdIdx = storeMetadata.getFieldIndex(DatastoreConstants.CHUNK__PARENT_DICO_ID);
      @SuppressWarnings("unchecked")
      final IWritableDictionary<Object> dicIdDictionary =
          (IWritableDictionary<Object>) dictionaryProvider.getDictionary(dicIdIdx);
      this.defaultDicId =
          dicIdDictionary.map(MemoryAnalysisDatastoreDescriptionConfig.DEFAULT_COMPONENT_ID_VALUE);
    }

    if (this.defaultIdxId < 0) {
      final int idxIdIdx = storeMetadata.getFieldIndex(DatastoreConstants.CHUNK__PARENT_INDEX_ID);
      @SuppressWarnings("unchecked")
      final IWritableDictionary<Object> idxIdDictionary =
          (IWritableDictionary<Object>) dictionaryProvider.getDictionary(idxIdIdx);
      this.defaultIdxId =
          idxIdDictionary.map(MemoryAnalysisDatastoreDescriptionConfig.DEFAULT_COMPONENT_ID_VALUE);
    }

    if (this.defaultRefId < 0) {
      final int refIdIdx = storeMetadata.getFieldIndex(DatastoreConstants.CHUNK__PARENT_REF_ID);
      @SuppressWarnings("unchecked")
      final IWritableDictionary<Object> refIdDictionary =
          (IWritableDictionary<Object>) dictionaryProvider.getDictionary(refIdIdx);
      this.defaultRefId =
          refIdDictionary.map(MemoryAnalysisDatastoreDescriptionConfig.DEFAULT_COMPONENT_ID_VALUE);
    }
  }

  private int getPartition(final IRecordReader record) {
    final int idx = record.getFormat().getFieldIndex(DatastoreConstants.CHUNK__PARTITION_ID);
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

  private String getChunkClassName(
      final IRecordReader record,
      final IDictionaryProvider dictionaryProvider,
      final IStoreMetadata storeMetadata) {
    final int partitionIdx = storeMetadata.getFieldIndex(DatastoreConstants.CHUNK__CLASS);
    final int idx = record.getFormat().getFieldIndex(DatastoreConstants.CHUNK__CLASS);
    @SuppressWarnings("unchecked")
    final IWritableDictionary<Object> refIdDictionary =
        (IWritableDictionary<Object>) dictionaryProvider.getDictionary(partitionIdx);

    return (String) refIdDictionary.read((Integer) record.read(idx));
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
