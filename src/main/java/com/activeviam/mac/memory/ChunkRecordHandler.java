/*
 * (C) ActiveViam 2007-2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.memory;

import com.activeviam.database.datastore.api.description.IDuplicateKeyHandler;
import com.activeviam.database.datastore.api.description.IKeyEventContext;
import com.activeviam.tech.chunks.internal.impl.TombStoneChunk;
import com.activeviam.tech.dictionaries.api.IDictionaryProvider;
import com.activeviam.tech.dictionaries.avinternal.IWritableDictionary;
import com.activeviam.tech.records.api.IRecordFormat;
import com.activeviam.tech.records.api.IRecordReader;
import com.activeviam.tech.records.api.IWritableRecord;

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

  private IRecordReader createMergedRecord(
      IRecordReader duplicateRecord,
      IRecordReader previousRecord,
      IKeyEventContext keyEventContext) {

    final int currentPartition = getPartition(previousRecord);
    final int currentDicId = getDicId(previousRecord);
    final int currentRefId = getRefId(previousRecord);
    final int currentIdxId = getIdxId(previousRecord);
    init(previousRecord, keyEventContext.getDictionaryProvider());

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
        if (getChunkClassName(duplicateRecord, keyEventContext.getDictionaryProvider())
            .contains(TombStoneChunk.class.getName())) {
          return duplicateRecord;
        }
        final IWritableRecord newRecord = copyRecord(duplicateRecord);

        assert newPartition != MemoryAnalysisDatastoreDescriptionConfig.NO_PARTITION;
        assert currentPartition != MemoryAnalysisDatastoreDescriptionConfig.NO_PARTITION;
        final int partitionIdx = getPartition(newRecord);
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
      final IRecordReader recordReader, final IDictionaryProvider dictionaryProvider) {
    if (this.sharedPartitionId < 0) {
      final int dicIdIdx = getPartition(recordReader);
      @SuppressWarnings("unchecked")
      final IWritableDictionary<Object> partitionDictionary =
          (IWritableDictionary<Object>) dictionaryProvider.getDictionary(dicIdIdx);
      this.sharedPartitionId =
          partitionDictionary.map(MemoryAnalysisDatastoreDescriptionConfig.MANY_PARTITIONS);
    }
    if (this.defaultDicId < 0) {
      final int dicIdIdx = getDicId(recordReader);
      @SuppressWarnings("unchecked")
      final IWritableDictionary<Object> dicIdDictionary =
          (IWritableDictionary<Object>) dictionaryProvider.getDictionary(dicIdIdx);
      this.defaultDicId =
          dicIdDictionary.map(MemoryAnalysisDatastoreDescriptionConfig.DEFAULT_COMPONENT_ID_VALUE);
    }
    if (this.defaultIdxId < 0) {
      final int idxIdIdx = getIdxId(recordReader);
      @SuppressWarnings("unchecked")
      final IWritableDictionary<Object> idxIdDictionary =
          (IWritableDictionary<Object>) dictionaryProvider.getDictionary(idxIdIdx);
      this.defaultIdxId =
          idxIdDictionary.map(MemoryAnalysisDatastoreDescriptionConfig.DEFAULT_COMPONENT_ID_VALUE);
    }

    if (this.defaultRefId < 0) {
      final int refIdIdx = getRefId(recordReader);
      @SuppressWarnings("unchecked")
      final IWritableDictionary<Object> refIdDictionary =
          (IWritableDictionary<Object>) dictionaryProvider.getDictionary(refIdIdx);
      this.defaultRefId =
          refIdDictionary.map(MemoryAnalysisDatastoreDescriptionConfig.DEFAULT_COMPONENT_ID_VALUE);
    }
  }

  private int getPartition(final IRecordReader recordReader) {
    return recordReader.getFormat().getFieldIndex(DatastoreConstants.CHUNK__PARTITION_ID);
  }

  private int getDicId(final IRecordReader recordReader) {
    return recordReader.getFormat().getFieldIndex(DatastoreConstants.CHUNK__PARENT_DICO_ID);
  }

  private int getIdxId(final IRecordReader recordReader) {
    return recordReader.getFormat().getFieldIndex(DatastoreConstants.CHUNK__PARENT_INDEX_ID);
  }

  private int getRefId(final IRecordReader recordReader) {
    return recordReader.getFormat().getFieldIndex(DatastoreConstants.CHUNK__PARENT_REF_ID);
  }

  private String getChunkClassName(
      final IRecordReader recordReader, final IDictionaryProvider dictionaryProvider) {
    final int partitionIdx = getPartition(recordReader);
    final int idx = recordReader.getFormat().getFieldIndex(DatastoreConstants.CHUNK__CLASS);
    @SuppressWarnings("unchecked")
    final IWritableDictionary<Object> refIdDictionary =
        (IWritableDictionary<Object>) dictionaryProvider.getDictionary(partitionIdx);

    return (String) refIdDictionary.read((Integer) recordReader.read(idx));
  }

  private IWritableRecord copyRecord(final IRecordReader record) {
    final IRecordFormat format = record.getFormat();
    final IWritableRecord newRecord = format.newRecord();
    for (int i = 0, end = format.getFieldCount(); i < end; i += 1) {
      newRecord.write(i, record.read(i));
    }

    return newRecord;
  }

  @Override
  public IRecordReader selectDuplicateKeyInDatastore(
      final IRecordReader duplicateRecord,
      final IRecordReader previousRecord,
      final IKeyEventContext keyEventContext) {
    return createMergedRecord(duplicateRecord, previousRecord, keyEventContext);
  }
}
