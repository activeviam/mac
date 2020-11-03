/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.memory;

import com.activeviam.fwk.ActiveViamRuntimeException;
import com.qfs.chunk.IArrayReader;
import com.qfs.chunk.IArrayWriter;
import com.qfs.concurrent.ICompletionSync;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.dic.IDictionary;
import com.qfs.store.query.ICursor;
import com.qfs.store.query.IPartitionedResultAcceptor;
import com.qfs.store.query.condition.impl.RecordQuery;
import com.qfs.store.record.IRecordFormat;
import com.qfs.store.record.IRecordReader;
import com.qfs.store.selection.impl.Selection;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.qfs.store.transaction.IOpenedTransaction;
import com.qfs.store.transaction.ITransactionManager.IUpdateWhereProcedure;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.Data;

public class DatastoreCalculations {

  private DatastoreCalculations() {
  }

  public static void fillLatestEpochColumn(
      IOpenedTransaction transactionManager,
      IDictionary<Object> epochDictionary,
      IDictionary<Object> chunkDictionary,
      IDictionary<Object> dumpNameDictionary)
      throws DatastoreTransactionException {
    final MaxEpochIdAcceptor maxEpochIdAcceptor = new MaxEpochIdAcceptor(epochDictionary);

    final ICompletionSync completion = transactionManager
        .getQueryRunner()
        .forQuery(
            new RecordQuery(DatastoreConstants.CHUNK_STORE,
                BaseConditions.TRUE,
                DatastoreConstants.CHUNK_ID,
                DatastoreConstants.CHUNK__DUMP_NAME,
                DatastoreConstants.VERSION__EPOCH_ID))
        .withAcceptor(maxEpochIdAcceptor)
        .run();

    completion.awaitCompletion();

    transactionManager.updateWhere(
        new Selection(
            DatastoreConstants.CHUNK_STORE,
            DatastoreConstants.CHUNK_ID,
            DatastoreConstants.CHUNK__DUMP_NAME,
            DatastoreConstants.VERSION__EPOCH_ID,
            DatastoreConstants.CHUNK__IS_LATEST_EPOCH),
        BaseConditions.TRUE,
        new UpdateLatestColumnProcedure(maxEpochIdAcceptor.getResult(),
            chunkDictionary,
            dumpNameDictionary));
  }

  private static class MaxEpochIdAcceptor implements IPartitionedResultAcceptor {

    private final ConcurrentMap<EpochIdKey, Long> maxima;
    private final IDictionary<?> epochDictionary;

    public MaxEpochIdAcceptor(IDictionary<?> epochDictionary) {
      maxima = new ConcurrentHashMap<>();
      this.epochDictionary = epochDictionary;
    }

    @Override
    public void onResult(int partitionId, ICursor result) {
      final IRecordFormat format = result.getRecordFormat();
      final int chunkIdIndex = format.getFieldIndex(DatastoreConstants.CHUNK_ID);
      final int dumpNameIndex = format.getFieldIndex(DatastoreConstants.CHUNK__DUMP_NAME);
      final int epochIdIndex = format.getFieldIndex(DatastoreConstants.VERSION__EPOCH_ID);

      for (final IRecordReader reader : result) {
        maxima.merge(
            new EpochIdKey(reader.readInt(chunkIdIndex),
                reader.readInt(dumpNameIndex)),
            epochDictionary.readLong(reader.readInt(epochIdIndex)),
            Math::max);
      }
    }

    @Override
    public void complete() {
    }

    @Override
    public void completeExceptionally(Throwable ex) {
      throw new ActiveViamRuntimeException(ex);
    }

    public Map<EpochIdKey, Long> getResult() {
      return maxima;
    }
  }

  @Data
  private static class EpochIdKey {

    private final int chunkId;
    private final int dumpName;
  }

  private static class UpdateLatestColumnProcedure implements IUpdateWhereProcedure {

    private int fieldIndex;
    private final Map<EpochIdKey, Long> maxEpochIds;
    private final IDictionary<Object> chunkDictionary;
    private final IDictionary<Object> dumpNameDictionary;

    public UpdateLatestColumnProcedure(
        Map<EpochIdKey, Long> maxEpochIds,
        IDictionary<Object> chunkDictionary,
        IDictionary<Object> dumpNameDictionary) {
      this.maxEpochIds = maxEpochIds;
      this.chunkDictionary = chunkDictionary;
      this.dumpNameDictionary = dumpNameDictionary;
    }

    @Override
    public void init(IRecordFormat selectionFormat, IRecordFormat storeFormat) {
      this.fieldIndex = storeFormat.getFieldIndex(DatastoreConstants.CHUNK__IS_LATEST_EPOCH);
    }

    @Override
    public void execute(IArrayReader selectedRecord, IArrayWriter recordWriter) {
      EpochIdKey key = new EpochIdKey(
          chunkDictionary.getPosition(selectedRecord.read(0)),
          dumpNameDictionary.getPosition(selectedRecord.read(1)));

      recordWriter.writeBoolean(
          this.fieldIndex,
          maxEpochIds.get(key).equals(selectedRecord.readLong(2)));
    }
  }
}
