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
import com.qfs.condition.impl.BaseConditions;
import com.qfs.store.query.ICursor;
import com.qfs.store.query.IDictionaryCursor;
import com.qfs.store.query.IPartitionedResultAcceptor;
import com.qfs.store.query.condition.impl.RecordQuery;
import com.qfs.store.record.IRecordFormat;
import com.qfs.store.record.IRecordReader;
import com.qfs.store.selection.impl.Selection;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.qfs.store.transaction.IOpenedTransaction;
import com.qfs.store.transaction.ITransactionManager.IUpdateWhereProcedure;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.Data;

public class DatastoreCalculations {

  private DatastoreCalculations() {
  }

  public static void fillLatestEpochColumn(IOpenedTransaction transactionManager)
      throws DatastoreTransactionException {
    final IDictionaryCursor cursor = transactionManager
        .getQueryRunner()
        .forQuery(
            new RecordQuery(DatastoreConstants.CHUNK_STORE,
                BaseConditions.TRUE,
                DatastoreConstants.CHUNK_ID,
                DatastoreConstants.CHUNK__DUMP_NAME,
                DatastoreConstants.VERSION__EPOCH_ID))
        //				.withAcceptor(new MaxEpochIdAcceptor())
        .run();

    final Map<Key, Long> maxEpochIds = new HashMap<>();
    for (IRecordReader reader : cursor) {
      long id = reader.readLong(0);
      String dump = (String) reader.read(1);
      long epoch = reader.readLong(2);

      Key key = new Key(id, dump);
      Long current = maxEpochIds.putIfAbsent(key, epoch);
      if (current != null && current.compareTo(epoch) < 0) {
        maxEpochIds.put(key, epoch);
      }
    }

    transactionManager.updateWhere(
        new Selection(
            DatastoreConstants.CHUNK_STORE,
            DatastoreConstants.CHUNK_ID,
            DatastoreConstants.CHUNK__DUMP_NAME,
            DatastoreConstants.VERSION__EPOCH_ID,
            "latest"),
        BaseConditions.TRUE,
        new UpdateLatestColumnProcedure(maxEpochIds));
  }

  private static class MaxEpochIdAcceptor implements IPartitionedResultAcceptor {

    private final ConcurrentMap<Key, Long> maxima;

    public MaxEpochIdAcceptor() {
      maxima = new ConcurrentHashMap<>();
    }

    @Override
    public void onResult(int partitionId, ICursor result) {
      final IRecordReader reader = result.getRecord();
      maxima.merge(
          new Key(
              (Long) reader.read(DatastoreConstants.CHUNK_ID),
              (String) reader.read(DatastoreConstants.CHUNK__DUMP_NAME)),
          (Long) reader.read(DatastoreConstants.VERSION__EPOCH_ID),
          Math::max);
    }

    @Override
    public void complete() {
    }

    @Override
    public void completeExceptionally(Throwable ex) {
      throw new ActiveViamRuntimeException(ex);
    }

    public Map<Key, Long> getResult() {
      return maxima;
    }
  }

  @Data
  private static class Key {

    private final Long chunkId;
    private final String dumpName;
  }

  private static class UpdateLatestColumnProcedure implements IUpdateWhereProcedure {

    private int fieldIndex;
    private final Map<Key, Long> maxEpochIds;

    public UpdateLatestColumnProcedure(Map<Key, Long> maxEpochIds) {
      this.maxEpochIds = maxEpochIds;
    }

    @Override
    public void init(IRecordFormat selectionFormat, IRecordFormat storeFormat) {
      this.fieldIndex = storeFormat.getFieldIndex("latest");
    }

    @Override
    public void execute(IArrayReader selectedRecord, IArrayWriter recordWriter) {
      Key key = new Key(selectedRecord.readLong(0),
          (String) selectedRecord.read(1));

      recordWriter.writeBoolean(
          this.fieldIndex, maxEpochIds.get(key)
              .equals(selectedRecord.readLong(2)));
    }
  }
}
