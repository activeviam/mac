/*
 * (C) ActiveViam 2013-2015
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.cfg.impl;

import com.activeviam.activepivot.server.spring.api.config.IActivePivotConfig;
import com.activeviam.activepivot.server.spring.api.config.IActivePivotContentServiceConfig;
import com.activeviam.activepivot.server.spring.api.config.IDatastoreConfig;
import com.activeviam.mac.cfg.security.impl.SecurityConfig;
import com.activeviam.tech.core.api.agent.AgentException;
import com.activeviam.web.spring.internal.JMXEnabler;
import com.activeviam.web.spring.internal.config.JwtConfig;
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
      JwtConfig.class,
      ManagerDescriptionConfig.class,

      // Pivot
      ActivePivotWithDatastoreConfig.class,

      // Content server
      ContentServiceConfig.class,

      // Specific to monitoring server
      SecurityConfig.class,
      SourceConfig.class,
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

  /**
   * Initialize and start the ActivePivot Manager, after performing all the injections into the
   * ActivePivot plug-ins.
   *
   * @return void
   */
  @Bean
  public Void startManager() {
    this.contentServiceConfig.loadPredefinedBookmarks();

    /* *********************************************** */
    /* Initialize the ActivePivot Manager and start it */
    /* *********************************************** */
    try {
      this.apConfig.activePivotManager().init(null);
      this.apConfig.activePivotManager().start();
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
    this.sourceConfig.watchStatisticDirectory();
  }

  /**
   * Enables JMX Monitoring for the Source.
   *
   * @return the {@link JMXEnabler} attached to the source
   */
  @Bean
  public JMXEnabler jmxMonitoringConnectorEnabler() {
    return new JMXEnabler("StatisticSource", this.sourceConfig);
  }

  /**
   * [Bean] JMX Bean to export bookmarks.
   *
   * @return the MBean
   */
  @Bean
  public JMXEnabler jmxBookmarkEnabler() {
    return new JMXEnabler("Bookmark", this.contentServiceConfig);
  }
}
