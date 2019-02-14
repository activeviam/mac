/*
 * (C) Quartet FS 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.cfg.impl;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Collection;
import java.util.logging.Logger;

import com.qfs.logging.MessagesDatastore;
import com.qfs.msg.csv.ICsvDataProvider;
import com.qfs.msg.csv.IFileEvent;
import com.qfs.msg.csv.filesystem.impl.DirectoryCSVTopic;
import com.qfs.msg.impl.WatcherService;
import com.qfs.pivot.monitoring.impl.MemoryMonitoringService;
import com.quartetfs.fwk.QuartetRuntimeException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.env.Environment;

/**
 * @author Quartet FS
 */
@Configuration
public class SourceConfig {

	/** Logger */
	private static final Logger LOGGER = MessagesDatastore.getLogger(SourceConfig.class);

	@Autowired
	protected MonitoringConnectorConfig connectorConfig;

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

	private void processEvent(final IFileEvent<Path> event) {
		if (event != null && event.created() != null) {
			// Load stat
			Collection<? extends ICsvDataProvider<Path>> providers = event.created();

			for (ICsvDataProvider<Path> provider : providers) {
				Path path = provider.getFileInfo().getIdentifier();
				File file = path.toFile();
				try {
					String message = connectorConfig.feedDatastore(
							MemoryMonitoringService.loadDumpedStatistic(file),
							file.getName().replaceAll("\\.[^.]*$", ""));
					LOGGER.info(message);
				} catch (Exception e) {
					throw new QuartetRuntimeException(e);
				}
			}
		}
	}
}
