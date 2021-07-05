/*
 * (C) ActiveViam 2013-2015
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.cfg.impl;

import com.activeviam.mac.cfg.security.impl.SecurityConfig;
import com.activeviam.mac.cfg.security.impl.UserConfig;
import com.activeviam.properties.cfg.impl.ActiveViamPropertyFromSpringConfig;
import com.qfs.pivot.content.impl.DynamicActivePivotContentServiceMBean;
import com.qfs.pivot.monitoring.impl.MemoryAnalysisService;
import com.qfs.server.cfg.IActivePivotConfig;
import com.qfs.server.cfg.IDatastoreConfig;
import com.qfs.server.cfg.content.IActivePivotContentServiceConfig;
import com.qfs.server.cfg.i18n.impl.LocalI18nConfig;
import com.qfs.server.cfg.impl.ActivePivotConfig;
import com.qfs.server.cfg.impl.ActivePivotServicesConfig;
import com.qfs.server.cfg.impl.ActivePivotXmlaServletConfig;
import com.qfs.server.cfg.impl.ActiveViamRestServicesConfig;
import com.qfs.server.cfg.impl.ActiveViamWebSocketServicesConfig;
import com.qfs.server.cfg.impl.DatastoreConfig;
import com.qfs.server.cfg.impl.FullAccessBranchPermissionsManagerConfig;
import com.qfs.server.cfg.impl.JwtConfig;
import com.qfs.server.cfg.impl.JwtRestServiceConfig;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.monitoring.jmx.impl.JMXEnabler;
import java.nio.file.Paths;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;

/**
 * Spring configuration of the ActivePivot Sandbox application.
 *
 * <p>We use {@link PropertySource} annotation(s) to define some .properties file(s), whose content
 * will be loaded into the Spring {@link Environment}, allowing some externally-driven configuration
 * of the application. Parameters can be quickly changed by modifying the {@code sandbox.properties}
 * file.
 *
 * <p>We use {@link Import} annotation(s) to reference additional Spring {@link Configuration}
 * classes, so that we can manage the application configuration in a modular way (split by
 * domain/feature, re-use of core config, override of core config, customized config, etc...).
 *
 * <p>Spring best practices recommends not to have arguments in bean methods if possible. One should
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
      ManagerDescriptionConfig.class,

      // Pivot
      ActivePivotConfig.class,
      DatastoreConfig.class,
      NoWriteDatastoreServiceConfig.class,
      FullAccessBranchPermissionsManagerConfig.class,
      ActivePivotServicesConfig.class,
      ActiveViamRestServicesConfig.class,
      ActiveViamWebSocketServicesConfig.class,
      ActivePivotXmlaServletConfig.class,

      // Content server
      ContentServiceConfig.class,
      LocalI18nConfig.class,

      // Specific to monitoring server
      SecurityConfig.class,
      UserConfig.class,
      SourceConfig.class,
      ActiveUiResourceServerConfig.class,
    })
public class MacServerConfig {

  /** Datastore spring configuration. */
  @Autowired protected IDatastoreConfig datastoreConfig;

  /** ActivePivot spring configuration. */
  @Autowired protected IActivePivotConfig apConfig;

  /** ActivePivot content service spring configuration. */
  @Autowired protected IActivePivotContentServiceConfig apContentServiceConfig;

  /** Content Service configuration. */
  @Autowired protected ContentServiceConfig contentServiceConfig;

  /** Spring configuration of the source files of the Memory Analysis Cube application. */
  @Autowired protected SourceConfig sourceConfig;

  /** Spring environment, automatically wired. */
  @Autowired protected Environment env;

  /** The name of the property that holds the boolean stating if the debug mode is activated or not. */
  public static Boolean DEBUG_PROPERTY;


  /**
   * Initialize and start the ActivePivot Manager, after performing all the injections into the
   * ActivePivot plug-ins.
   *
   * @return void
   */
  @Bean
  public Void startManager() {
    DEBUG_PROPERTY = env.getProperty("server.debug") != null &&
        Boolean.parseBoolean(env.getProperty("server.debug"));
    contentServiceConfig.loadPredefinedBookmarks();

    /* *********************************************** */
    /* Initialize the ActivePivot Manager and start it */
    /* *********************************************** */
    try {
      apConfig.activePivotManager().init(null);
      apConfig.activePivotManager().start();
    } catch (AgentException e) {
      throw new IllegalStateException("Cannot start the application", e);
    }

    return null;
  }

  /**
   * Hook called after the application started.
   *
   * <p>It performs every operation once the application is up and read, such as loading data, etc.
   */
  @EventListener(ApplicationReadyEvent.class)
  public void afterStart() {
    // Connect the real-time updates
    sourceConfig.watchStatisticDirectory();
  }

  /**
   * Enables JMX Monitoring for the Source.
   *
   * @return the {@link JMXEnabler} attached to the source
   */
  @Bean
  public JMXEnabler jmxMonitoringConnectorEnabler() {
    return new JMXEnabler("StatisticSource", sourceConfig);
  }

  /**
   * Enable JMX Monitoring for the Datastore.
   *
   * @return the {@link JMXEnabler} attached to the datastore
   */
  @Bean
  public JMXEnabler jmxDatastoreEnabler() {
    return new JMXEnabler(datastoreConfig.datastore());
  }

  /**
   * Enable JMX Monitoring for ActivePivot Components.
   *
   * @return the {@link JMXEnabler} attached to the activePivotManager
   */
  @Bean
  public JMXEnabler jmxActivePivotEnabler() {
    startManager();

    return new JMXEnabler(apConfig.activePivotManager());
  }

  /**
   * [Bean] JMX Bean to export bookmarks.
   *
   * @return the MBean
   */
  @Bean
  public JMXEnabler jmxBookmarkEnabler() {
    return new JMXEnabler("Bookmark", contentServiceConfig);
  }

  /**
   * Enable JMX Monitoring for the ContentService.
   *
   * @return the {@link JMXEnabler} attached to the Content Service
   */
  @Bean
  public JMXEnabler jmxActivePivotContentServiceEnabler() {
    // to allow operations from the JMX bean
    return new JMXEnabler(
        new DynamicActivePivotContentServiceMBean(
            apContentServiceConfig.activePivotContentService(), apConfig.activePivotManager()));
  }

  /**
   * Enable Memory JMX Monitoring.
   *
   * @return the {@link JMXEnabler} attached to the memory analysis service.
   */
  @Bean
  public JMXEnabler jmxMemoryMonitoringServiceEnabler() {
    return new JMXEnabler(
        new MemoryAnalysisService(
            this.datastoreConfig.datastore(),
            this.apConfig.activePivotManager(),
            this.datastoreConfig.datastore().getEpochManager(),
            Paths.get(System.getProperty("java.io.tmpdir"))));
  }
}
