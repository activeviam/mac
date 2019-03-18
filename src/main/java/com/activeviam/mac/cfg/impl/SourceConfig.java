/*
 * (C) Quartet FS 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.cfg.impl;

import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.statistic.memory.visitor.impl.FeedVisitor;
import com.qfs.jmx.JmxOperation;
import com.qfs.logging.MessagesDatastore;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.msg.csv.ICsvDataProvider;
import com.qfs.msg.csv.IFileEvent;
import com.qfs.msg.csv.filesystem.impl.DirectoryCSVTopic;
import com.qfs.msg.impl.WatcherService;
import com.qfs.pivot.monitoring.impl.MemoryStatisticSerializerUtil;
import com.qfs.store.IDatastore;
import com.qfs.store.impl.StoreUtils;
import com.qfs.store.transaction.IDatastoreSchemaTransactionInformation;
import com.quartetfs.fwk.QuartetRuntimeException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.env.Environment;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Optional;
import java.util.logging.Logger;

/**
 * @author Quartet FS
 */
@Configuration
public class SourceConfig {

	/** Logger */
	private static final Logger LOGGER = MessagesDatastore.getLogger(SourceConfig.class);

	@Autowired
	protected IDatastore datastore;

	/** Spring environment, automatically wired */
	@Autowired
	protected Environment env;

	@Bean
	@Lazy
	public DirectoryCSVTopic statisticTopic() {
		return new DirectoryCSVTopic(
				"StatisticTopic",
				null,
				env.getRequiredProperty("statistic.folder"),
				// Process json files, compressed or not
				FileSystems.getDefault().getPathMatcher("glob:**.json*"),
				new WatcherService());
	}

	public void loadStatistics() {
		final IFileEvent<Path> initialEvent = statisticTopic().fullReload();
		processEvent(initialEvent);
	}

	/**
	 * Start to watch the folder that contains statistics.
	 *
	 * @return {@link Void}
	 */
	public void watchStatisticDirectory() {
		statisticTopic().listen((__, event) -> processEvent(event));
	}

	public void processEvent(final IFileEvent<Path> event) {
		if (event != null && event.created() != null) {
			// Load stat
			Collection<? extends ICsvDataProvider<Path>> providers = event.created();

			for (ICsvDataProvider<Path> provider : providers) {
				final Path path = provider.getFileInfo().getIdentifier();
				final File file = path.toFile();
				try {
					String message = feedDatastore(
							MemoryStatisticSerializerUtil.readStatisticFile(file),
							file.getName().replaceAll("\\.[^.]*$", ""));
					LOGGER.info(message);
				} catch (Exception e) {
					throw new QuartetRuntimeException(e);
				}
			}
		}
	}

	/**
	 * @param memoryStatistic {@link IMemoryStatistic} to load.
	 * @return message to the user
	 */
	public String feedDatastore(final IMemoryStatistic memoryStatistic, final String dumpName) {
		final Optional<IDatastoreSchemaTransactionInformation> info = this.datastore.edit(tm -> {
			memoryStatistic.accept(new FeedVisitor(this.datastore.getSchemaMetadata(), tm, dumpName));
		});


		if (info.isPresent()) {
//			SchemaPrinter.printStore(this.datastore, DatastoreConstants.DICTIONARY_STORE);
//			SchemaPrinter.printStore(this.datastore, DatastoreConstants.PROVIDER_STORE);
			return "Commit successful at epoch "
					+ info.get().getId()
					+ ". Datastore size "
					+ StoreUtils.getSize(datastore.getHead(), DatastoreConstants.CHUNK_STORE);
		} else {
			return "Issue during the commit";
		}
	}

	@JmxOperation(
			desc = "Load statistic from file.",
			name = "Load a IMemoryStatistic",
			params = { "path" })
	public String loadDumpedStatistic(String path) throws IOException {
		return feedDatastore(
				MemoryStatisticSerializerUtil.readStatisticFile(Paths.get(path).toFile()),
				Paths.get(path).getFileName().toString().replaceAll("\\.[^.]*$", ""));
	}
}
