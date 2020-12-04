/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription.ParentType;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.service.monitoring.IMemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.qfs.store.query.ICursor;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestMemoryStatisticLoadingWithReverseOrder extends ATestMemoryStatistic {

  @Test
  public void testLoadDatastoreStats() {
    createApplication(
        (monitoredDatastore, monitoredManager) -> {
          fillApplication(monitoredDatastore);

          final IMemoryAnalysisService analysisService =
              createService(monitoredDatastore, monitoredManager);
          final Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
          final List<? extends IMemoryStatistic> storeStats =
              new ArrayList<>(loadMemoryStatFromFolder(exportPath).getChildren());

          Collections.shuffle(storeStats);

          Assertions.assertThat(storeStats).isNotEmpty();
          assertLoadsCorrectly(storeStats, getClass());
        });
  }

  @Test
  public void testLoadDatastoreStatsWithVectors() {
    createApplicationWithVector(
        true,
        (monitoredDatastore, monitoredManager) -> {
          commitDataInDatastoreWithVectors(monitoredDatastore, false);

          final IMemoryAnalysisService analysisService =
              createService(monitoredDatastore, monitoredManager);
          final Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
          final List<? extends IMemoryStatistic> storeStats =
              new ArrayList<>(loadMemoryStatFromFolder(exportPath).getChildren());

          Collections.shuffle(storeStats);

          Assertions.assertThat(storeStats).isNotEmpty();
          final IDatastore monitoringDatastore = assertLoadsCorrectly(storeStats, getClass());

          final Set<Long> vectorBlocks = extractVectorBlocks(monitoringDatastore);
          assertVectorBlockAttributesArePresent(monitoringDatastore, vectorBlocks);
        });
  }

  protected void assertVectorBlockAttributesArePresent(
      final IDatastore monitoringDatastore, final Collection<Long> chunkIdSubset) {
    final ICursor cursor =
        monitoringDatastore
            .getHead()
            .getQueryRunner()
            .forStore(DatastoreConstants.CHUNK_STORE)
            .withCondition(
                BaseConditions.And(
                    BaseConditions.In(DatastoreConstants.CHUNK_ID, chunkIdSubset.toArray()),
                    BaseConditions.Equal(DatastoreConstants.CHUNK__VECTOR_BLOCK_LENGTH, null)))
            .selecting(DatastoreConstants.CHUNK_ID)
            .onCurrentThread()
            .run();

    Assertions.assertThat(StreamSupport.stream(cursor.spliterator(), false).count()).isZero();
  }

  protected Set<Long> extractVectorBlocks(final IDatastore monitoringDatastore) {
    final ICursor cursor =
        monitoringDatastore
            .getHead()
            .getQueryRunner()
            .forStore(DatastoreConstants.CHUNK_STORE)
            .withCondition(
                BaseConditions.Equal(DatastoreConstants.OWNER__COMPONENT, ParentType.VECTOR_BLOCK))
            .selecting(DatastoreConstants.CHUNK_ID)
            .onCurrentThread()
            .run();

    return StreamSupport.stream(cursor.spliterator(), false)
        .map(reader -> reader.readLong(0))
        .collect(Collectors.toSet());
  }
}
