/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.statistic.memory;

import java.io.StringReader;
import java.util.Arrays;
import java.util.HashSet;

import com.activeviam.health.monitor.IHealthCheckAgent;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription;
import com.activeviam.mac.statistic.memory.visitor.impl.FeedVisitor;
import com.qfs.QfsWebUtils;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.junit.EnvTestRule;
import com.qfs.junit.ResourceRule;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.impl.MemoryStatisticBuilder;
import com.qfs.pivot.monitoring.impl.MonitoringStatisticSerializerUtil;
import com.qfs.pivot.servlet.impl.ContextValueFilter;
import com.qfs.rest.client.impl.ClientPool;
import com.qfs.rest.services.impl.JsonRestService;
import com.qfs.server.cfg.IActivePivotConfig;
import com.qfs.server.cfg.IDatastoreConfig;
import com.qfs.server.cfg.impl.JettyServer;
import com.qfs.server.cfg.impl.MonitoringRestServicesConfig;
import com.qfs.service.store.impl.NoSecurityDatastoreServiceConfig;
import com.qfs.store.IDatastore;
import com.qfs.store.build.impl.DatastoreBuilder;
import com.qfs.store.build.impl.UnitTestDatastoreBuilder;
import com.qfs.store.query.impl.DatastoreQueryHelper;
import com.qfs.store.service.impl.DatastoreService;
import com.qfs.util.MemUtils;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.security.IContextValueManager;
import com.quartetfs.biz.pivot.security.IContextValuePropagator;
import com.quartetfs.fwk.security.ISecurityFacade;
import com.quartetfs.fwk.security.impl.SecurityDetails;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import test.scenario.MultipleStores;
import test.scenario.MultipleStoresData;

public class TestRemoteMemoryStatisticLoading {

	@ClassRule
	public static final EnvTestRule envRule = EnvTestRule.getInstance();
	@ClassRule
	public static final ResourceRule resources = new ResourceRule();

	protected static final String datastoreBeanName = "datastore";
	protected static final String jsonDatastoreServiceBeanName = "jsonDatastoreService";
	private static JettyServer jettyServer;
	private static ClientPool clientPool;
	private static JsonRestService service;
	private static IDatastore datastore;

	@Rule
	public TemporaryFolder temp = new TemporaryFolder();

	@Configuration
	@Import(value = {
			MonitoringRestServicesConfig.class,
			TestOffHeapMonitoringServiceConfig.DatastoreConfigForTest.class,
			TestOffHeapMonitoringServiceConfig.APConfigForTest.class
	})
	protected static class TestOffHeapMonitoringServiceConfig {

		@Configuration
		protected static class DatastoreConfigForTest implements IDatastoreConfig {

			@Bean(name = datastoreBeanName)
			@Override
			public IDatastore datastore() {
				return new UnitTestDatastoreBuilder()
						.setSchemaDescription(MultipleStores.schemaDescription())
						.build();
			}

			@Bean(name = jsonDatastoreServiceBeanName)
			public DatastoreService jsonDatastoreService() {
				final ISecurityFacade sf = Mockito.mock(ISecurityFacade.class);
				Mockito.when(sf.snapshotSecurityDetails()).thenReturn(
						new SecurityDetails("user1", new HashSet<>(Arrays.asList("ROLE_USER", "role1"))));

				return new DatastoreService(
						datastore(),
						new NoSecurityDatastoreServiceConfig(),
						sf);
			}

		}

		@Configuration
		protected static class APConfigForTest implements IActivePivotConfig {

			@Override
			public IActivePivotManagerDescription activePivotManagerDescription() {
				return null;
			}

			@Bean
			@Override
			public IActivePivotManager activePivotManager() {
				// As of Spring 5.0, Beans cannot be null anymore;
				// see: https://stackoverflow.com/questions/49044770/change-in-how-spring-5-handles-null-beans
				final IActivePivotManager manager = Mockito.mock(IActivePivotManager.class);
				Mockito.when(manager.getMemoryStatistic()).thenReturn(
						new MemoryStatisticBuilder()
								.withName("mock")
								.withCreatorClasses(APConfigForTest.class)
								.withMemoryFootPrint(0, 0)
								.build());
				return manager;
			}

			@Override
			public IHealthCheckAgent healthCheckAgent() {
				return null;
			}

			@Override
			public ContextValueFilter contextValueFilter() {
				return null;
			}

			@Override
			public IContextValuePropagator contextValuePropagator() {
				return null;
			}

			@Override
			public IContextValueManager contextValueManager() {
				return null;
			}
		}

	}

	/**
	 * The {@link DatastoreService} that will be used
	 * (as a local service but it could be remote)
	 */
	protected static DatastoreService jsonDatastoreService;

	@BeforeClass
	// Override super to only add one rest service with a different datastore configuration
	public static void setUp() throws Exception {
		MemUtils.runGC();

		jettyServer = new JettyServer(TestOffHeapMonitoringServiceConfig.class);

		jettyServer.createServer();
		jettyServer.start();

		clientPool = new ClientPool(1);
		service = new JsonRestService(QfsWebUtils.url("http://localhost:" + jettyServer.getPort(),
		                                              MonitoringRestServicesConfig.REST_API_URL_PREFIX,
		                                              "/"), clientPool);

		datastore = (IDatastore) jettyServer.getApplicationContext().getBean(datastoreBeanName);
		MultipleStoresData.addDataSingle(MultipleStoresData.generateData(2, 5, 0), datastore.getTransactionManager());

		jsonDatastoreService = (DatastoreService) jettyServer.getApplicationContext().getBean(jsonDatastoreServiceBeanName);
	}

	@AfterClass
	public static void tearDownTestOffHeap() {
		datastore = null;
		if (clientPool != null) {
			clientPool.close();
			clientPool = null;
		}
		jsonDatastoreService = null;
		if (jettyServer != null) {
			jettyServer.stop();
			jettyServer = null;
		}
	}

	@Test
	public void testFillMonitoringDatastoreFromRemoteServer() throws Exception {
		// MONITORING DATASTORE
		final IDatastoreSchemaDescription desc = new MemoryAnalysisDatastoreDescription();
		final IDatastore monitoringDatastore = new DatastoreBuilder()
				.setSchemaDescription(desc)
				.build();

		final String data = service.path("memory_allocations").get().as(String.class);
		final IMemoryStatistic stats = MonitoringStatisticSerializerUtil.deserialize(
				new StringReader(data),
				IMemoryStatistic.class);

		final IMemoryStatistic datastoreStats = stats.getChildren().stream()
				.filter(s -> MemoryStatisticConstants.STAT_NAME_DATASTORE.equals(s.getName()))
				.findFirst()
				.orElseThrow(() -> new AssertionError("No stats for the datastore in " + data));

		monitoringDatastore.edit(tm -> {
			datastoreStats.accept(new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "test"));
		});

		for (final String storeName : monitoringDatastore.getSchemaMetadata().getStoreNames()) {
			final int cursorSize = DatastoreQueryHelper.getCursorSize(
					monitoringDatastore.getHead().getQueryRunner()
							.forStore(storeName)
							.withoutCondition()
							.selectingAllStoreFields()
							.run());
			Assertions.assertThat(cursorSize).isGreaterThanOrEqualTo(1); // check store is non empty
		}
	}

}
