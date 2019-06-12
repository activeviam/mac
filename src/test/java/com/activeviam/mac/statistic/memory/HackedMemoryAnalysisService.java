package com.activeviam.mac.statistic.memory;

/*
 * (C) ActiveViam 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

import static com.qfs.monitoring.statistic.memory.MemoryStatisticConstants.ATTR_NAME_DATE;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.qfs.fwk.services.BadArgumentException;
import com.qfs.jmx.JmxAttribute;
import com.qfs.jmx.JmxOperation;
import com.qfs.logging.MessagesDatastore;
import com.qfs.memory.impl.PlatformOperations;
import com.qfs.monitoring.memory.IMemoryMonitored;
import com.qfs.monitoring.statistic.impl.LongStatisticAttribute;
import com.qfs.monitoring.statistic.impl.StringStatisticAttribute;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.IMemoryStatisticBuilder;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.PivotMemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.impl.MemoryStatisticBuilder;
import com.qfs.multiversion.IEpochHistory;
import com.qfs.multiversion.IEpochManager;
import com.qfs.multiversion.IMultiVersion;
import com.qfs.multiversion.IVersionHistory;
import com.qfs.multiversion.impl.AEpochManager;
import com.qfs.pivot.monitoring.impl.MemoryStatisticSerializerUtil;
import com.qfs.service.monitoring.IMemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.qfs.store.IDatastoreSchemaMetadata;
import com.qfs.store.IDatastoreSchemaVersion;
import com.qfs.store.IDictionaryManager;
import com.qfs.store.IMultiVersionDatastoreSchema;
import com.qfs.store.IStoreVersion;
import com.qfs.util.impl.QfsArrays;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.IActivePivotVersion;
import com.quartetfs.biz.pivot.IMultiVersionActivePivot;
import com.quartetfs.fwk.QuartetRuntimeException;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

/**
 * Basic implementation of the {@link IMemoryAnalysisService}.
 * @author ActiveViam
 */
public class HackedMemoryAnalysisService implements IMemoryAnalysisService {

	/** Logger */
	private static final Logger LOGGER = MessagesDatastore.getLogger(HackedMemoryAnalysisService.class);

	/** The monitored datastore */
	protected final IDatastore datastore;
	/** The monitored activePivotManager */
	protected final IActivePivotManager activePivotManager;
	/**
	 * The epoch manager to access to stats for specific epochs.
	 */
	protected final IEpochManager epochManager;

	/** Directory into which the memory statistics are exported. */
	protected final Path exportDirectory;

	/** File name prefix for a given store statistic */
	public static final String STORE_FILE_PREFIX = "store_";

	/** File name prefix for a given pivot statistic */
	public static final String PIVOT_FILE_PREFIX = "pivot_";

	/**
	 * Full constructor.
	 * @param datastore datastore to consider
	 * @param activePivotManager ActivePivot manoger to consider.
	 * @param epochManager the epoch manager to export stats on specific epochs
	 */
	public HackedMemoryAnalysisService(
			final IDatastore datastore,
			final IActivePivotManager activePivotManager,
			final IEpochManager epochManager,
			final Path exportDirectory) {
		this.datastore = datastore;
		this.activePivotManager = activePivotManager;
		this.epochManager = epochManager;
		this.exportDirectory = exportDirectory;
	}

	@Override
	public IMemoryStatistic getApplicationMemoryStatistic() {
		if (LOGGER.isLoggable(Level.INFO)) {
			LOGGER.log(Level.INFO, "Starting to retrieve memory statistics.");
		}
		final long start = System.nanoTime();

		final IMemoryStatistic result = new MemoryStatisticBuilder()
				.withChildren(getComponentsStatistics())
				.withName("application")
				.withCurrentDate()
				.withCreatorClasses(getClass())
				.build();

		final long total = System.nanoTime() - start;
		if (LOGGER.isLoggable(Level.INFO)) {
			LOGGER.log(Level.INFO, "Retrieving memory statistics took " + TimeUnit.NANOSECONDS.toMillis(total) + " ms");
		}

		return result;
	}

	/**
	 * @return folder where to dump files.
	 */
	protected String getDumpFolder() {
		return this.exportDirectory.toAbsolutePath().toString();
	}

	@Override
	public Path exportMostRecentVersion(final String folderSuffix) {
		final Date exportDate = new Date();
		final Path path = createExportFolder(folderSuffix);

		final IDatastoreSchemaMetadata schemaMetadata = datastore.getSchemaMetadata();
		final Map<String, Long> globalReport = collectGlobalMemoryStatus();
		final int storeCount = schemaMetadata.getStoreCount();
		for (int i = 0; i < storeCount; i++) {
			final String storeName = schemaMetadata.getStoreMetadata(i).getName();
			final IMemoryStatistic memoryStatisticForStore = datastore.getMemoryStatisticForStore(i);
			completeWithGlobalMemoryStats(memoryStatisticForStore, globalReport);
			dumpSingleStatistic(path, memoryStatisticForStore, STORE_FILE_PREFIX + storeName, exportDate);
		}

		final Map<String, IMultiVersionActivePivot> activePivots = activePivotManager.getActivePivots();
		for (IMultiVersionActivePivot pivot : activePivots.values()) {
			final IMemoryStatistic pivotStatistic = pivot.getMemoryStatistic();
			pivotStatistic.getAttributes().putIfAbsent(
					PivotMemoryStatisticConstants.ATTR_NAME_MANAGER_ID,
					new StringStatisticAttribute(this.activePivotManager.getName()));
			completeWithGlobalMemoryStats(pivotStatistic, globalReport);
			final String pivotId = pivot.getId();
			dumpSingleStatistic(path, pivotStatistic, PIVOT_FILE_PREFIX + pivotId, exportDate);
		}

		return path;
	}

	@Override
	public Path exportApplication(final String folderSuffix) {
		final Map<IMultiVersion, Collection<IMemoryMonitored>> selection = new HashMap<>();
		final Set<? extends Entry<IMultiVersion, ? extends IEpochHistory>> histories = epochManager.getHistories().entrySet();

		// we should have one multiversion for the datastore and one for each cube.
		for (final Entry<IMultiVersion, ? extends IEpochHistory> history : histories) {
			final IMultiVersion multiVersion = history.getKey();
			if (!(history.getValue() instanceof IVersionHistory)) {
				LOGGER.warning("Unable to dump statistics for " + multiVersion + ".\nIts epoch history is not a version history.");
				continue;
			}
			final IVersionHistory<?> epochHistory = (IVersionHistory<?>) history.getValue();

			if (multiVersion instanceof IMultiVersionDatastoreSchema) {
				// we dump a different file for each store
				final IDatastoreSchemaMetadata schemaMetadata = ((IMultiVersionDatastoreSchema) multiVersion).getMetadata();
				final int storeCount = schemaMetadata.getStoreCount();
				for (int i = 0; i < storeCount; i++) {
					epochHistory.forEachVersion(v -> {
						final IDatastoreSchemaVersion schemaVersion = (IDatastoreSchemaVersion) v;
						selection.computeIfAbsent(multiVersion, __ -> new ArrayList<>()).add(schemaVersion);
						return true;
					});
				}
			} else if (multiVersion instanceof IMultiVersionActivePivot) {
				epochHistory.forEachVersion(v -> {
					if (!(v instanceof IMemoryMonitored)) {
						LOGGER.warning("Unable to dump statistics for " + v + ".\nIt is not memory monitored.");
						return false;
					} else {
						final IActivePivotVersion pivotVersion = (IActivePivotVersion) v;
						selection.computeIfAbsent(multiVersion, __ -> new ArrayList<>()).add(pivotVersion);
					}
					return true;
				});
			}
		}

		return export(folderSuffix, selection);
	}

	@Override
	public Path exportBranches(final String folderSuffix, final Set<String> selectedBranches) {
		final Map<IMultiVersion, Collection<IMemoryMonitored>> selection = new HashMap<>();

		// For the datastore, we must explore the history
		final Map<IMultiVersion, ? extends IEpochHistory> histories = this.epochManager.getHistories();
		final List<IMultiVersionDatastoreSchema> schemas = histories.keySet().stream()
				.filter(IMultiVersionDatastoreSchema.class::isInstance)
				.map(IMultiVersionDatastoreSchema.class::cast)
				.collect(Collectors.toList());
		if (schemas.size() == 1) {
			final IMultiVersionDatastoreSchema schema = schemas.get(0);
			final IVersionHistory<?> history = (IVersionHistory<?>) histories.get(schema);
			final Collection<IMemoryMonitored> datastoreVersions = new ArrayList<>();
			final Set<String> remainingBranches = new HashSet<>(selectedBranches);
			history.forEachVersion(version -> {
				if (remainingBranches.remove(version.getEpoch().getBranch())) {
					datastoreVersions.add((IDatastoreSchemaVersion) version);
				}
				return !remainingBranches.isEmpty();
			});

			if (!datastoreVersions.isEmpty()) {
				selection.put(schema, datastoreVersions);
			}
		} else {
			throw new BadArgumentException("The selected branches do not match any branch. Provided: " + selectedBranches);
		}

		for (final IMultiVersionActivePivot pivot : this.activePivotManager.getActivePivots().values()) {
			final Collection<IMemoryMonitored> pivotBranches = selectedBranches.stream()
					.map(branch -> pivot.getHead(branch))
					.filter(Objects::nonNull)
					.collect(Collectors.toList());
			if (!pivotBranches.isEmpty()) {
				selection.put(pivot, pivotBranches);
			}
		}

		return export(folderSuffix, selection);
	}

	@Override
	public Path exportVersions(final String folderSuffix, final long[] selectedEpochs) {
		final Map<IMultiVersion, Collection<IMemoryMonitored>> selection = new HashMap<>();
		final Set<? extends Entry<IMultiVersion, ? extends IEpochHistory>> histories = epochManager.getHistories().entrySet();

		// we should have one multiversion for the datastore and one for each cube.
		for (final Entry<IMultiVersion, ? extends IEpochHistory> history : histories) {
			final IMultiVersion multiVersion = history.getKey();
			if (!(history.getValue() instanceof IVersionHistory)) {
				LOGGER.warning("Unable to dump statistics for " + multiVersion + ".\nIts epoch history is not a version history.");
				continue;
			}
			final IVersionHistory<?> epochHistory = (IVersionHistory<?>) history.getValue();

			if (multiVersion instanceof IMultiVersionDatastoreSchema) {
				// we dump a different file for each store
				final IDatastoreSchemaMetadata schemaMetadata = ((IMultiVersionDatastoreSchema) multiVersion).getMetadata();
				final int storeCount = schemaMetadata.getStoreCount();
				for (int i = 0; i < storeCount; i++) {
					TLongSet epochs = new TLongHashSet(selectedEpochs);
					epochHistory.forEachVersion(v -> {
						final boolean shouldDumpStat = epochs.remove(v.getEpochId());
						if (shouldDumpStat) {
							final IDatastoreSchemaVersion schemaVersion = (IDatastoreSchemaVersion) v;
							selection.computeIfAbsent(multiVersion, __ -> new ArrayList<>()).add(schemaVersion);
						}
						return !epochs.isEmpty();
					});
				}
			} else if (multiVersion instanceof IMultiVersionActivePivot) {
				TLongSet epochs = new TLongHashSet(selectedEpochs);
				epochHistory.forEachVersion(v -> {
					final boolean shouldDumpStat = epochs.remove(v.getEpochId());
					if (shouldDumpStat) {
						if (!(v instanceof IMemoryMonitored)) {
							LOGGER.warning("Unable to dump statistics for " + v + ".\nIt is not memory monitored.");
							return false;
						} else {
							final IActivePivotVersion pivotVersion = (IActivePivotVersion) v;
							selection.computeIfAbsent(multiVersion, __ -> new ArrayList<>()).add(pivotVersion);
						}
					}
					return !epochs.isEmpty();
				});
			}
		}

		return export(folderSuffix, selection);
	}

	/**
	 * Exports to a given directory the statistics for all selected elements.
	 * @param folderSuffix name of a folder to hold the statistics
	 * @param elements elements to export
	 * @return the absolute path to the export folder
	 */
	protected Path export(
			final String folderSuffix,
			final Map<? extends IMultiVersion, ? extends Collection<? extends IMemoryMonitored>> elements) {
		Date exportDate = new Date();
		final Path path = createExportFolder(folderSuffix);

		final Map<String, Long> globalReport = collectGlobalMemoryStatus();
		for (final Entry<? extends IMultiVersion, ? extends Collection<? extends IMemoryMonitored>> history : elements.entrySet()) {
			final IMultiVersion multiVersion = history.getKey();
			if (multiVersion instanceof IMultiVersionDatastoreSchema) {
				// we dump a different file for each store
				final IMultiVersionDatastoreSchema schema = (IMultiVersionDatastoreSchema) multiVersion;
				final IDatastoreSchemaMetadata schemaMetadata = schema.getMetadata();
				final int storeCount = schemaMetadata.getStoreCount();

				// Prepare the dictionary stats for each store
				final IDictionaryManager dictionaryManager = schema.getDictionaryManager();
				for (int i = 0; i < storeCount; i++) {
					final String storeName = schemaMetadata.getStoreMetadata(i).getName();
					final IMemoryStatistic dictionaryStat = dictionaryManager.getMemoryStatisticForStore(storeName);

					final IMemoryStatisticBuilder statisticBuilder = new MemoryStatisticBuilder()
						.withName(MemoryStatisticConstants.STAT_NAME_MULTIVERSION_STORE)
						.withCreatorClasses(HackedMemoryAnalysisService.class);
					for (final IMemoryMonitored v : history.getValue()) {
						final IDatastoreSchemaVersion schemaVersion = (IDatastoreSchemaVersion) v;
						final IStoreVersion storeVersion = schemaVersion.getStore(storeName);
						final IMemoryStatistic storeVersionMemoryStatistic = storeVersion.getMemoryStatistic();
						// REVIEW it may be too much to add the dictionary statistics for each version
						// when it is shared by all versions.
						// However, it is easier for the feeder on the other hand
						storeVersionMemoryStatistic.append(dictionaryStat);

						statisticBuilder.withChild(storeVersionMemoryStatistic);
					}

					final IMemoryStatistic versionStatistic = statisticBuilder.build();
					completeWithGlobalMemoryStats(versionStatistic, globalReport);
					final String statisticFileName = MemoryStatisticConstants.STAT_NAME_MULTIVERSION_STORE + "_" +
							storeName.replaceAll("\\s+", "_");
					dumpSingleStatistic(path, versionStatistic, statisticFileName, exportDate);
				}

			} else if (multiVersion instanceof IMultiVersionActivePivot) {
				final IMultiVersionActivePivot mvPivot = (IMultiVersionActivePivot) multiVersion;
				final IMemoryStatisticBuilder statisticBuilder = new MemoryStatisticBuilder()
					.withName(PivotMemoryStatisticConstants.STAT_NAME_MULTIVERSION_PIVOT)

					.withCreatorClasses(HackedMemoryAnalysisService.class);
					

				for (final IMemoryMonitored v : history.getValue()) {
					final IMemoryStatistic memoryStatistic = v.getMemoryStatistic();
					statisticBuilder.withChild(memoryStatistic);
				}
				final IMemoryStatistic stat = statisticBuilder.build();
				stat.getAttributes().putIfAbsent(
						PivotMemoryStatisticConstants.ATTR_NAME_MANAGER_ID,
						new StringStatisticAttribute(this.activePivotManager.getName()));
				completeWithGlobalMemoryStats(stat, globalReport);
				final String statisticFileName = PivotMemoryStatisticConstants.STAT_NAME_MULTIVERSION_PIVOT + "_"
						+ mvPivot.getId().replaceAll("\\s+", "_");
				dumpSingleStatistic(path, stat, statisticFileName, exportDate);
			}
		}

		return path;
	}

	protected Path createExportFolder(final String folderSuffix) {
		final String dumpFolder = getDumpFolder();
		Path path = Paths.get(dumpFolder, folderSuffix);
		while (path.toFile().exists()) {
			LOGGER.info("Folder " + folderSuffix + " already exist at this location " + dumpFolder);
			// If the file already exist, change the name of the file.
			path = Paths.get(dumpFolder, folderSuffix + "_" + System.currentTimeMillis());
			LOGGER.info("Dump folder will be created with the name " + path.getFileName());
		}

		// Create the target directory
		path.toFile().mkdirs();

		return path;
	}

	/**
	 * @return The statistics for each component of the application
	 */
	protected List<IMemoryStatistic> getComponentsStatistics() {
		return Arrays.asList(
				datastore.getMemoryStatistic(),
				activePivotManager.getMemoryStatistic());
	}

	/**
	 * @return folder where to dump files.
	 */
	@JmxAttribute(desc = "Folder path of the dump files.")
	public String getJmxDumpFolder() {
		return getDumpFolder();
	}

	/**
	 * Build the memory statistics of the application and dump them
	 * into a folder with the given name in the temp directory.
	 * The statistics are separated in different files that are compressed.
	 * The statistics are based on the current epoch, so if there's been a rebuild or dropped partitions,
	 * we might not see some components that could still account for memory used in the application.
	 *
	 * @param folderName name of the dump folder.
	 * @return the full path to the dump folder.
	 */
	@JmxOperation(desc = "Build the global memory statistics of the application and dump it "
			+ "into a folder with the given name in the temp directory. The statistics is based on the most recent version, "
			+ "e.g. dropped partitions or rebuilt indices that are not yet discarded "
			+ "but don't belong to the most recent version are not taken into account.",
			name = "Dump memory statistics",
			params = {"folderName"})
	public String jmxDumpStatisticsForMostRecentVersion(String folderName) {
		try {
			final Path resultPath = exportMostRecentVersion(folderName);

			return resultPath.toAbsolutePath().toString();
		} catch (QuartetRuntimeException e) {
			LOGGER.log(Level.WARNING, "Unable to dump statistic", e);
			return null;
		}
	}

	/**
	 * Build the memory statistics of the application and dump them
	 * into a folder with the given name in the temp directory.
	 * The statistics are separated in different files that are compressed.
	 *
	 * @param folderName name of the dump folder.
	 * @param epochsRanges The list of ranges of epochs, for example: <code>0,1,2,3</code> or
	 * 	 *        <code>0-12</code> or <code>0-3,12-15/code>
	 * @return the full path to the dump folder.
	 */
	@JmxOperation(desc = "Build the global memory statistics of the application and dump it "
			+ "into a folder with the given name in the temp directory. "
			+ "All the given epochs are taken into account for the statistics. "
			+ "It is advised to use the current epoch",
			name = "Dump memory statistics for some epochs",
			params = {"folderName", "epochsRanges"})
	public String jmxDumpStatisticsForEpochs(final String folderName, final String epochsRanges) {
		Date exportDate = new Date();
		String dumpFolder = getDumpFolder();
		Path path = Paths.get(dumpFolder, folderName);
		while (path.toFile().exists()) {
			LOGGER.info("Folder " + folderName + " already exist at this location " + dumpFolder);
			// If the file already exist, change the name of the file.
			path = Paths.get(dumpFolder, folderName + "_" + System.currentTimeMillis());
			LOGGER.info("Dump folder will be created with the name " + path.getFileName());
		}

		// Create the target directory
		path.toFile().mkdirs();

		final Map<String, Long> globalReport = collectGlobalMemoryStatus();
		final long[] epochIds = AEpochManager.parseVersionsRanges(epochsRanges);
		final Set<? extends Entry<IMultiVersion, ? extends IEpochHistory>> histories = epochManager.getHistories().entrySet();
		// we should have one multiversion for the datastore and one for each cube.
		for (final Entry<IMultiVersion, ? extends IEpochHistory> history : histories) {
			final IMultiVersion multiVersion = history.getKey();
			if (!(history.getValue() instanceof IVersionHistory)) {
				LOGGER.warning("Unable to dump statistics for " + multiVersion + ".\nIts epoch history is not a version history.");
				continue;
			}
			final IVersionHistory<?> epochHistory = (IVersionHistory<?>) history.getValue();

			if (multiVersion instanceof IMultiVersionDatastoreSchema) {
				// we dump a different file for each store
				final IDatastoreSchemaMetadata schemaMetadata = ((IMultiVersionDatastoreSchema) multiVersion).getMetadata();
				final int storeCount = schemaMetadata.getStoreCount();
				for (int i = 0; i < storeCount; i++) {
					String storeName = schemaMetadata.getStoreMetadata(i).getName();
					MemoryStatisticBuilder statisticBuilder = new MemoryStatisticBuilder();
					statisticBuilder.withName(MemoryStatisticConstants.STAT_NAME_STORE);
					statisticBuilder.withCreatorClasses(HackedMemoryAnalysisService.class);
					TLongSet epochs = new TLongHashSet(epochIds);
					epochHistory.forEachVersion(v -> {
						final boolean shouldDumpStat = epochs.remove(v.getEpochId());
						if (shouldDumpStat) {
							final IDatastoreSchemaVersion schemaVersion = (IDatastoreSchemaVersion) v;
							final IStoreVersion storeVersion = schemaVersion.getStore(storeName);
							final IMemoryStatistic storeVersionMemoryStatistic = storeVersion.getMemoryStatistic();
							statisticBuilder.withChild(storeVersionMemoryStatistic);
						}
						return !epochs.isEmpty();
					});

					final IMemoryStatistic versionStatistic = statisticBuilder.build();
					completeWithGlobalMemoryStats(versionStatistic, globalReport);
					dumpSingleStatistic(path, versionStatistic, STORE_FILE_PREFIX + storeName, exportDate);
				}

			} else {
				TLongSet epochs = new TLongHashSet(epochIds);
				MemoryStatisticBuilder statisticBuilder = new MemoryStatisticBuilder();
				if (multiVersion instanceof IMultiVersionActivePivot) {
					statisticBuilder.withName(PIVOT_FILE_PREFIX + ((IMultiVersionActivePivot) multiVersion).getId());
				}
				statisticBuilder.withCreatorClasses(HackedMemoryAnalysisService.class);
				epochHistory.forEachVersion(v -> {
					final boolean shouldDumpStat = epochs.remove(v.getEpochId());
					if (shouldDumpStat) {
						if (!(v instanceof IMemoryMonitored)) {
							LOGGER.warning("Unable to dump statistics for " + v + ".\nIt is not memory monitored.");
							return false;
						}
						final IMemoryStatistic memoryStatistic = ((IMemoryMonitored) v).getMemoryStatistic();
						statisticBuilder.withChild(memoryStatistic);
					}
					return !epochs.isEmpty();
				});
				final IMemoryStatistic stat = statisticBuilder.build();
				completeWithGlobalMemoryStats(stat, globalReport);
				dumpSingleStatistic(path, stat, stat.getName(), exportDate);
			}
		}

		return path.toAbsolutePath().toString();
	}

	/**
	 * Dumps the given statistics in a compressed file, adding the exportDate to the statistic.
	 * @param dumpDirectory Where to dump the file.
	 * @param stat The stat to enrich with the export date and to dump.
	 * @param name The name of the file (without the extension)
	 * @param exportDate The date to register within the stat before dumping it.
	 */
	protected void dumpSingleStatistic(
			final Path dumpDirectory,
			final IMemoryStatistic stat,
			final String name,
			final Date exportDate) {
		stat.getAttributes().put(
				ATTR_NAME_DATE,
				new LongStatisticAttribute(exportDate.getTime() / 1000));

		try {
			MemoryStatisticSerializerUtil.writeStatisticFile(
					stat,
					dumpDirectory,
					name);
		} catch (final IOException e) {
			LOGGER.log(Level.WARNING, "Unable to dump statistic " + name, e);
		}
	}

	/**
	 * Dumps the content provided by the consumer into the provided file.
	 *
	 * @param str the {@link String} to write in the dumped file.
	 * @param file the file in which the dump must occur.
	 * @return the path to the file.
	 * @throws IOException if we failed to create the temp file to write the content into.
	 */
	protected static String dumpInFile(final Consumer<Writer> str, final File file) throws IOException {
		// We first write the content to a temp file, and then move it
		// to the actual destination. Otherwise someone could mistakenly
		// start reading the file before it is created.
		final File tmpFile = createTemporaryFile(file);
		final PrintWriter writer = new PrintWriter(tmpFile, "UTF-8");
		str.accept(writer);
		writer.flush();
		writer.close();

		// Perform an atomic move.
		// On most OSes, it is equivalent to File.renameTo(), which only changes the name of the file not its directory.
		// We use Files.move() because File.renameTo() is platform-dependent and it does not work on Windows yet.
		Files.move(tmpFile.toPath(), file.toPath(), StandardCopyOption.ATOMIC_MOVE);

		return file.getAbsolutePath();
	}

	/**
	 * Creates a temporary file in the same directory as {@code file}, with the same name, but
	 * adding .tmp to the extension.
	 *
	 * @param file The file for which to create a .tmp surrogate.
	 * @return a temporary file
	 */
	protected static File createTemporaryFile(final File file) {
		File f = Paths.get(file.getAbsolutePath() + ".tmp").toFile();
		while (f.exists()) {
			f = new File(f.getAbsolutePath() + "_" + System.currentTimeMillis() + ".tmp");
		}
		return f;
	}

	/**
	 * Collects the general values of memory consumption.
	 * <p>
	 * This provides the total quantity of on-heap and off-heap used memory as well as
	 * the configured limits for the memory.
	 * </p>
	 * <p>
	 * This is meant to be added as attributes to top-level statistics, to provide information on the system
	 * from which they were extracted.
	 * </p>
	 * @return the general application memory statistics
	 */
	public Map<String, Long> collectGlobalMemoryStatus() {
		final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
		final long maxHeap = memoryBean.getHeapMemoryUsage().getMax();
		final long usedHeap = memoryBean.getHeapMemoryUsage().getUsed();
		final List<BufferPoolMXBean> beans = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
		BufferPoolMXBean directMemoryBean = null;
		for (BufferPoolMXBean bean : beans) {
			if (bean.getName().equals("direct")) {
				directMemoryBean = bean;
			}
		}
		final long directMemory = directMemoryBean != null ? directMemoryBean.getMemoryUsed() : -1;
		final long maxDirectMemory = PlatformOperations.maxDirectMemory();

		return QfsArrays.mutableMap(
				MemoryStatisticConstants.STAT_NAME_GLOBAL_USED_HEAP_MEMORY, usedHeap,
				MemoryStatisticConstants.ST$AT_NAME_GLOBAL_MAX_HEAP_MEMORY, maxHeap,
				MemoryStatisticConstants.STAT_NAME_GLOBAL_USED_DIRECT_MEMORY, directMemory,
				MemoryStatisticConstants.STAT_NAME_GLOBAL_MAX_DIRECT_MEMORY, maxDirectMemory);
	}

	/**
	 * Includes general memory info as attributes of the given statistics.
	 * <p>
	 * This is meant to consume the statistics created by {@link #collectGlobalMemoryStatus()}.
	 * </p>
	 * @param statistic statistics to complete
	 * @param globalReport memory report
	 */
	protected void completeWithGlobalMemoryStats(final IMemoryStatistic statistic, final Map<String, Long> globalReport) {
		globalReport.forEach((attribute, value) -> {
			statistic.getAttributes().put(
					attribute,
					new LongStatisticAttribute(value));
		});
	}
}

