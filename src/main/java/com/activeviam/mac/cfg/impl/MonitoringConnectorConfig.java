/*
 * (C) Quartet FS 2013-2014
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.cfg.impl;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Optional;

import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.statistic.memory.visitor.impl.DatastoreFeederVisitor;
import com.activeviam.mac.statistic.memory.visitor.impl.FeedVisitor;
import com.qfs.QfsWebUtils;
import com.qfs.jmx.JmxOperation;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.pivot.monitoring.impl.MemoryMonitoringService;
import com.qfs.pivot.monitoring.impl.MonitoringStatisticSerializerUtil;
import com.qfs.rest.client.impl.ClientPool;
import com.qfs.rest.client.impl.UserAuthenticator;
import com.qfs.rest.services.IRestService;
import com.qfs.rest.services.impl.JsonRestService;
import com.qfs.server.cfg.impl.MonitoringRestServicesConfig;
import com.qfs.store.IDatastore;
import com.qfs.store.NoTransactionException;
import com.qfs.store.impl.StoreUtils;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.qfs.store.transaction.IDatastoreSchemaTransactionInformation;
import com.quartetfs.fwk.monitoring.jmx.impl.JMXEnabler;
import com.quartetfs.fwk.serialization.SerializerException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/**
 * Spring configuration of the connector (REST & JMX).
 *
 * @author Quartet FS
 */
@Configuration("connector")
public class MonitoringConnectorConfig {

	/** Spring environment, automatically wired */
	@Autowired
	protected Environment env;

	protected IRestService restService;

	/** Credentials for sandbox server. */
	public static final String USERNAME = "admin";
	public static final String PASSWORD = "admin";

	@Autowired
	protected IDatastore datastore;

	@SuppressWarnings("resource")
	@Bean
	protected IRestService restService() {
		return restService = new JsonRestService(
				baseUrl(),
				new ClientPool(
						2,
						Collections.singletonList(new UserAuthenticator(USERNAME, PASSWORD))));
	}

	@JmxOperation(
			name = "Load statistic from the remote server",
			desc = "Call to AP server to retrieve datastore statistics",
			params = {})
	public String jmxPollStatisticFromRemoteServer() throws Exception {
		// Rest call
		final String data = restService.path(
				QfsWebUtils.url(MonitoringRestServicesConfig.REST_API_URL_PREFIX, "memory_allocations"))
				.get().as(String.class);
		return feedDatastore(
				MonitoringStatisticSerializerUtil.deserialize(new StringReader(data), IMemoryStatistic.class),
				"remote");
	}

	@JmxOperation(
			desc = "Load statistic from file.",
			name = "Load a IMemoryStatistic",
			params = { "path" })
	public String loadDumpedStatistic(String path) throws IOException {
		return feedDatastore(
				MemoryMonitoringService.loadDumpedStatistic(path),
				Paths.get(path).getFileName().toString().replaceAll("\\.[^.]*$", ""));
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
			return "Commit successful at epoch "
					+ info.get().getId()
					+ ". Datastore size "
					+ StoreUtils.getSize(datastore.getHead(), DatastoreConstants.CHUNK_STORE);
		} else {
			return "Issue during the commit.";
		}
	}

	public String baseUrl() {
		return "http://" + env.getRequiredProperty("server.remote.uri");
	}

	@Bean
	public JMXEnabler JMXMonitoringConnectorEnabler() {
		return new JMXEnabler("MonitoringConnector", this);
	}

}
