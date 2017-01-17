/*
 * (C) Quartet FS 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.qfs.monitoring.cfg.impl;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Collection;
import java.util.logging.Logger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

import com.qfs.logging.MessagesDatastore;
import com.qfs.msg.csv.ICSVTopic;
import com.qfs.msg.csv.ICsvDataProvider;
import com.qfs.msg.csv.IFileEvent;
import com.qfs.msg.csv.IFileListener;
import com.qfs.msg.csv.filesystem.impl.DirectoryCSVTopic;
import com.qfs.msg.impl.WatcherService;
import com.qfs.pivot.monitoring.impl.MemoryMonitoringService;
import com.quartetfs.fwk.QuartetRuntimeException;

/**
 * @author Quartet FS
 */
public class MonitoringSourceConfig {

	/** Logger */
	private static final Logger LOGGER = MessagesDatastore.getLogger(MonitoringSourceConfig.class);

	@Autowired
	protected MonitoringConnectorConfig connectorConfig;

	/** Spring environment, automatically wired */
	@Autowired
	protected Environment env;

	/**
	 * Start to watch the folder that contains statistics.
	 *
	 * @return {@link Void}
	 */
	@Bean
	public Void watchStatisticDirectory() {
		DirectoryCSVTopic topic = new DirectoryCSVTopic("StatisticTopic",
				null,
				env.getRequiredProperty("statistic.folder"),
				FileSystems.getDefault().getPathMatcher("glob:**.json"),
				new WatcherService());

		topic.listen(new IFileListener<Path>() {

			@Override
			public void onEvent(ICSVTopic<Path> topic, IFileEvent<Path> event) {
				if (event != null && event.created() != null) {
					// Load stat
					Collection<? extends ICsvDataProvider<Path>> providers = event.created();

					for (ICsvDataProvider<Path> provider : providers) {
						Path path = provider.getFileInfo().getIdentifier();
						File file = path.toFile();
						try {
							String message = connectorConfig.feedDatastore(MemoryMonitoringService.loadDumpedStatistic(file));
							LOGGER.info(message);
						} catch (Exception e) {
							throw new QuartetRuntimeException(e);
						}
					}
				}
			}
		});

		return null;
	}
}
