/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.statistic.memory;

import com.qfs.dic.IDictionary;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.service.monitoring.IMemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.qfs.store.impl.Datastore;

import org.junit.Ignore;
import org.junit.Test;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import static org.junit.Assert.assertNotEquals;

public class TestMemoryStatisticLoading extends ATestMemoryStatistic {
	
	/**
	 * Assert the number of offheap chunks by filling the datastore used
	 * for monitoring AND doing a query on it for counting. Comparing
	 * the value from counting from {@link IMemoryStatistic}.
	 */
	@Test
	public void testLoadDatastoreStats() {
		createApplication((monitoredDatastore, monitoredManager) -> {
			fillApplication(monitoredDatastore);

			final IMemoryAnalysisService analysisService = createService(monitoredDatastore, monitoredManager);
			final Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
			final Collection<IMemoryStatistic> storeStats = loadDatastoreMemoryStatFromFolder(exportPath);
			assertNotEquals(0, storeStats.size());
			assertLoadsCorrectly(storeStats,getClass());
		});
	}
	
	public void doTestLoadMonitoringDatastoreWithVectors(boolean duplicateVectors) throws Exception {
		createApplicationWithVector(duplicateVectors, (monitoredDatastore, monitoredManager) -> {
			commitDataInDatastoreWithVectors(monitoredDatastore, duplicateVectors);

			final IMemoryAnalysisService analysisService = createService(monitoredDatastore, monitoredManager);
			final Path exportPath = analysisService.exportMostRecentVersion("doTestLoadMonitoringDatastoreWithVectors[" + duplicateVectors + "]");
			final Collection<IMemoryStatistic> datastoreStats = loadDatastoreMemoryStatFromFolder(exportPath);

			assertLoadsCorrectly(datastoreStats,getClass());
		});
	}

	@Test
	public void testLoadPivotStats() {
		createApplication((monitoredDatastore, monitoredManager) -> {
			fillApplication(monitoredDatastore);

			final IMemoryAnalysisService analysisService = createService(monitoredDatastore, monitoredManager);
			final Path exportPath = analysisService.exportMostRecentVersion("testLoadPivotStats");
			final Collection<IMemoryStatistic> pivotStats = loadPivotMemoryStatFromFolder(exportPath);
			assertNotEquals(0, pivotStats.size());
			assertLoadsCorrectly(pivotStats,getClass());
		});
	}

	@Test
	public void testLoadFullStats() {
		createApplication((monitoredDatastore, monitoredManager) -> {
			fillApplication(monitoredDatastore);

			final IMemoryAnalysisService analysisService = createService(monitoredDatastore, monitoredManager);
			final Path exportPath = analysisService.exportMostRecentVersion("testLoadFullStats");
			final IMemoryStatistic fullStats = loadMemoryStatFromFolder(exportPath);
			assertNotEquals(null, fullStats);
			assertLoadsCorrectly(fullStats);
		});
	}
	
	// FIXME : branches export does not properly work if EpochDimension is not defined in all the Cubes
	@Ignore
	@Test
	public void testLoadPivotsWithInconsistantEpochDimensions() {
		createMinimalApplicationForEpochDimensionFailure((monitoredDatastore,monitoredManager)->{
			Set<String> branchSet = new HashSet<>();
			branchSet.add("branch1");

			fillApplicationWithBranches(monitoredDatastore,branchSet,true);
			
			final IMemoryAnalysisService analysisService = createService(monitoredDatastore, monitoredManager);
			final Path exportPath = analysisService.exportBranches("testLoadFullStats", branchSet);
			final IMemoryStatistic fullStats = loadMemoryStatFromFolder(exportPath);
			assertNotEquals(null, fullStats);
			assertLoadsCorrectly(fullStats);	
		});
	}
	
	@Test
	public void testLoadFullStatsWithBranches() {
		createApplication((monitoredDatastore, monitoredManager) -> {
			
			Set<String> branchSet = new HashSet<>();
			branchSet.add("branch1");
			branchSet.add("branch2");

			fillApplicationWithBranches(monitoredDatastore,branchSet,false);
			
			//Also export master (?)
			branchSet.add("master");

			final IMemoryAnalysisService analysisService = createService(monitoredDatastore, monitoredManager);
			final Path exportPath = analysisService.exportBranches("testLoadFullStats", branchSet);
			final IMemoryStatistic fullStats = loadMemoryStatFromFolder(exportPath);
			assertNotEquals(null, fullStats);
			assertLoadsCorrectly(fullStats);
		});
	}
	
	@Test
	public void testLoadFullStatsWithEpochs() {
		createApplication((monitoredDatastore, monitoredManager) -> {
			
			Set<String> branchSet = new HashSet<>();
			branchSet.add("branch1");
			branchSet.add("branch2");
			fillApplication(monitoredDatastore);
			fillApplicationWithBranches(monitoredDatastore,branchSet,true);
			
			//Also export master (?)
			long epochs[] = new long[2];
			epochs[0]=1L;
			epochs[1]=100L;

			final IMemoryAnalysisService analysisService = createService(monitoredDatastore, monitoredManager);
			final Path exportPath = analysisService.exportVersions("testLoadFullStats", epochs);
			final IMemoryStatistic fullStats = loadMemoryStatFromFolder(exportPath);
			assertNotEquals(null, fullStats);
			assertLoadsCorrectly(fullStats);
		});
	}

	@Test
	public void testLoadMonitoringDatastoreWithVectorsWODuplicate() throws Exception {
		doTestLoadMonitoringDatastoreWithVectors(false);
	}

	@Test
	public void testLoadMonitoringDatastoreWithDuplicate() throws Exception {
		doTestLoadMonitoringDatastoreWithVectors(true);
	}
	
	/**
	 * Asserts the chunks number and off-heap memory as computed from the loaded datastore are consistent
	 * with the ones computed by visiting the statistic.
	 * @param statistic
	 */
	protected void assertLoadsCorrectly(IMemoryStatistic statistic) {
		assertLoadsCorrectly(Collections.singleton(statistic),getClass());
	}


}
