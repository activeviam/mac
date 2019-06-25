/*
 * (C) ActiveViam 2013-2015
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.cfg.impl;

import com.activeviam.mac.cfg.security.impl.CorsConfig;
import com.activeviam.mac.cfg.security.impl.SecurityConfig;
import com.activeviam.mac.cfg.security.impl.UserConfig;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.properties.cfg.impl.ActiveViamPropertyFromSpringConfig;
import com.qfs.pivot.content.impl.DynamicActivePivotContentServiceMBean;
import com.qfs.server.cfg.IActivePivotConfig;
import com.qfs.server.cfg.IDatastoreConfig;
import com.qfs.server.cfg.content.IActivePivotContentServiceConfig;
import com.qfs.server.cfg.i18n.impl.LocalI18nConfig;
import com.qfs.server.cfg.impl.ActivePivotConfig;
import com.qfs.server.cfg.impl.ActivePivotServicesConfig;
import com.qfs.server.cfg.impl.ActiveViamRestServicesConfig;
import com.qfs.server.cfg.impl.ActiveViamWebSocketServicesConfig;
import com.qfs.server.cfg.impl.DatastoreConfig;
import com.qfs.server.cfg.impl.FullAccessBranchPermissionsManagerConfig;
import com.qfs.server.cfg.impl.JwtConfig;
import com.qfs.server.cfg.impl.JwtRestServiceConfig;
import com.qfs.store.IDatastore;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.monitoring.jmx.impl.JMXEnabler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;


/**
 * Spring configuration of the ActivePivot Sandbox application.
 *
 * <p>
 * We use {@link PropertySource} annotation(s) to define some .properties file(s), whose content
 * will be loaded into the Spring {@link Environment}, allowing some externally-driven configuration
 * of the application. Parameters can be quickly changed by modifying the {@code sandbox.properties}
 * file.
 *
 * <p>
 * We use {@link Import} annotation(s) to reference additional Spring {@link Configuration} classes,
 * so that we can manage the application configuration in a modular way (split by domain/feature,
 * re-use of core config, override of core config, customized config, etc...).
 *
 * <p>
 * Spring best practices recommends not to have arguments in bean methods if possible. One should
 * rather autowire the appropriate spring configurations (and not beans directly unless necessary),
 * and use the beans from there.
 *
 * @author ActiveViam
 */
@Configuration
@Import(
		value = {
				ActiveViamPropertyFromSpringConfig.class,

				JwtRestServiceConfig.class,
				JwtConfig.class,

				DatastoreDescriptionConfig.class,
				ManagerDescriptionConfig.class,

				// Pivot
				ActivePivotConfig.class,
				DatastoreConfig.class,
				NoWriteDatastoreServiceConfig.class,
				FullAccessBranchPermissionsManagerConfig.class,
				ActivePivotServicesConfig.class,
				ActiveViamRestServicesConfig.class,
				ActiveViamWebSocketServicesConfig.class,

				// Content server
				LocalContentServiceConfig.class,
				LocalI18nConfig.class,

				// Specific to monitoring server
				SecurityConfig.class,
				CorsConfig.class,
				UserConfig.class,

				SourceConfig.class,

				ActiveUIResourceServerConfig.class
		})
public class MacServerConfig {

	/** Datastore spring configuration */
	@Autowired
	protected IDatastoreConfig datastoreConfig;

	/** ActivePivot spring configuration */
	@Autowired
	protected IActivePivotConfig apConfig;

	/** ActivePivot content service spring configuration */
	@Autowired
	protected IActivePivotContentServiceConfig apCSConfig;

	/** Spring configuration of the source files of the Memory Analysis Cube application*/
	@Autowired
	protected SourceConfig sourceConfig;

	/**
	 *
	 * Initialize and start the ActivePivot Manager, after performing all the injections into the
	 * ActivePivot plug-ins.
	 *
	 * @return void
	 */
	@Bean
	public Void startManager() {
		// Force the add of a Ref data in the datastore
		final IDatastore datastore = datastoreConfig.datastore();
		datastore.edit(tm->{
			tm.add(DatastoreConstants.CHUNK_TO_REF_STORE, "N/A", "N/A", -1L);
			tm.add(DatastoreConstants.CHUNK_TO_INDEX_STORE, "N/A", "N/A", -1L);
			tm.add(DatastoreConstants.CHUNK_TO_DICO_STORE, "N/A", "N/A", -1L);
		});

		/* *********************************************** */
		/* Initialize the ActivePivot Manager and start it */
		/* *********************************************** */
		try {
			apConfig.activePivotManager().init(null);
			apConfig.activePivotManager().start();
		} catch (AgentException e) {
			throw new IllegalStateException("Cannot start the application", e);
		}


		// Connect the real-time updates
		sourceConfig.watchStatisticDirectory();

		return null;
	}

	/**
	 * Enables JMX Monitoring for the Source
	 * @return the {@link JMXEnabler} attached to the source
	 */
	@Bean
	public JMXEnabler JMXMonitoringConnectorEnabler() {
		return new JMXEnabler("StatisticSource", sourceConfig);
	}

	/**
	 * Enable JMX Monitoring for the Datastore
	 *
	 * @return the {@link JMXEnabler} attached to the datastore
	 */
	@Bean
	public JMXEnabler JMXDatastoreEnabler() {
		return new JMXEnabler(datastoreConfig.datastore());
	}

	/**
	 * Enable JMX Monitoring for ActivePivot Components
	 *
	 * @return the {@link JMXEnabler} attached to the activePivotManager
	 */
	@Bean
	public JMXEnabler JMXActivePivotEnabler() {
		startManager();

		return new JMXEnabler(apConfig.activePivotManager());
	}

	/**
	 * Enable JMX Monitoring for the ContentService
	 *
	 * @return the {@link JMXEnabler} attached to the Content Service
	 */
	@Bean
	public JMXEnabler JMXActivePivotContentServiceEnabler() {
		// to allow operations from the JMX bean
		return new JMXEnabler(
				new DynamicActivePivotContentServiceMBean(
						apCSConfig.activePivotContentService(),
						apConfig.activePivotManager()));
	}

}
