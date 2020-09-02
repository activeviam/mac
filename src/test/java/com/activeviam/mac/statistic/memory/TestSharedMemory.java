package com.activeviam.mac.statistic.memory;

import com.quartetfs.fwk.AgentException;
import java.io.IOException;
import org.junit.Test;

public class TestSharedMemory extends ATestMemoryStatistic {

  /**
   * Tests that importing an application with a store and a pivot containing a level named after a
   * field sets some chunks as "shared" (the Dictionary chunks) as expected
   *
   * @throws AgentException
   * @throws IOException
   */
  @Test
  public void testShared() throws AgentException, IOException {
    // todo vlg
    throw new AssertionError();

//    final Pair<IDatastore, IActivePivotManager> monitoredApp =
//        createMicroApplicationWithLeafBitmap();
//
//    // Add records
//    monitoredApp
//        .getLeft()
//        .edit(
//            tm -> {
//              IntStream.range(0, 100)
//                  .forEach(
//                      i -> {
//                        tm.add("A", i * i);
//                      });
//            });
//
//    performGC();
//
//    // Force to discard all versions
//    monitoredApp.getLeft().getEpochManager().forceDiscardEpochs(__ -> true);
//
//    final IMemoryAnalysisService analysisService =
//        createService(monitoredApp.getLeft(), monitoredApp.getRight());
//    final Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
//    final IMemoryStatistic stats = loadMemoryStatFromFolder(exportPath);
//
//    // Start a monitoring datastore with the exported data
//    final IDatastore monitoringDatastore = createAnalysisDatastore();
//    monitoringDatastore.edit(
//        tm -> {
//          stats.accept(new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "data"));
//        });
//    // Query record chunks data :
//    final IDictionaryCursor cursor =
//        monitoringDatastore
//            .getHead()
//            .getQueryRunner()
//            .forStore(CHUNK_STORE)
//            .withCondition(BaseConditions.TRUE)
//            .selecting(DatastoreConstants.CHUNK__OWNER)
//            .onCurrentThread()
//            .run();
//    final List<String> list = new ArrayList<>();
//    cursor.forEach(
//        (record) -> {
//          final ChunkOwner owner = (ChunkOwner) record.read(0);
//          list.add(owner.getName());
//        });
//    Assertions.assertThat(list).contains("A", "Cube", SharedOwner.getInstance().getName());
  }
}
