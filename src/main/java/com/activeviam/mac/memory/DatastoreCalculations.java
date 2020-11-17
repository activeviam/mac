/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.memory;

import com.activeviam.fwk.ActiveViamRuntimeException;
import com.qfs.concurrent.ICompletionSync;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.desc.impl.DatastoreSchemaDescriptionUtil;
import com.qfs.dic.IDictionary;
import com.qfs.dic.ISchemaDictionaryProvider;
import com.qfs.store.query.ICursor;
import com.qfs.store.query.IPartitionedResultAcceptor;
import com.qfs.store.query.condition.impl.RecordQuery;
import com.qfs.store.transaction.IOpenedTransaction;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DatastoreCalculations {

  private DatastoreCalculations() {
  }

  public static void replicateChunksForMissingEpochs(
      final IOpenedTransaction transaction,
      final ISchemaDictionaryProvider dictionaries) {

    final IDictionary<Object> epochDictionary = dictionaries.getDictionary(
        DatastoreConstants.VERSION_STORE,
        DatastoreConstants.VERSION__EPOCH_ID);

    final DistinctAcceptor acceptor = new DistinctAcceptor(epochDictionary);
    final ICompletionSync epochsCursor = transaction.getQueryRunner().forQuery(
        new RecordQuery(DatastoreConstants.VERSION_STORE,
            BaseConditions.TRUE,
            DatastoreConstants.VERSION__EPOCH_ID))
        .withAcceptor(acceptor)
        .run();
    epochsCursor.awaitCompletion();

    final ICursor chunksWithEpochCursor = transaction.getQueryRunner().forQuery(
        new RecordQuery(DatastoreConstants.CHUNK_STORE,
            BaseConditions.TRUE,
            DatastoreConstants.CHUNK_ID,
            DatastoreConstants.CHUNK__DUMP_NAME,
            DatastoreConstants.VERSION__EPOCH_ID,
            DatastoreSchemaDescriptionUtil.createPath(
                MemoryAnalysisDatastoreDescription.CHUNK_TO_BRANCH,
                DatastoreConstants.VERSION__BRANCH_NAME)))
        .run();

    return;
  }

  protected static class DistinctAcceptor implements IPartitionedResultAcceptor {

    /** Partial results (one per partition) */
    protected final ConcurrentMap<Integer, Set<Long>> partialResults;

    /** The field dictionary, could be null if the field is not dictionarized */
    protected final IDictionary<Object> fieldDictionary;

    /** Final, merged result */
    protected final Set<Long> result;

    /**
     * Constructor
     *
     * @param fieldDictionary The field dictionary, could be null
     */
    public DistinctAcceptor(final IDictionary<Object> fieldDictionary) {
      this.partialResults = new ConcurrentSkipListMap<>();
      this.fieldDictionary = fieldDictionary;
      this.result = new HashSet<>();
    }

    @Override
    public void onResult(int partitionId, ICursor resultCursor) {
      // The partial result of a partition is contributed once by only one thread
      assert partialResults.get(partitionId) == null :
          "The partial result is already calculated for the partition " + partitionId;

      final Set<Long> partial = new HashSet<>();
      while (resultCursor.next()) {
        final Object v = (this.fieldDictionary != null)
            ? this.fieldDictionary.read(resultCursor.getRecord().readInt(0))
            : resultCursor.getRecord().read(0);
        partial.add((Long) v);
      }

      this.partialResults.put(partitionId, partial);
    }

    @Override
    public void complete() {
      // The complete is called once all the thread has finished calculating the partial results
      // So no risk of race condition here
      for (Set<Long> partial : partialResults.values()) {
        if (partial != null) {
          this.result.addAll(partial);
        }
      }
    }

    @Override
    public void completeExceptionally(Throwable ex) {
      throw new ActiveViamRuntimeException(ex);
    }

    /**
     * @return the final result
     */
    public Set<Long> getResult() {
      return this.result;
    }
  }
}
