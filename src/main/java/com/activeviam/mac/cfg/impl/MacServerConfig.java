/*
 * (C) ActiveViam 2013-2015
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.cfg.impl;

import com.activeviam.activepivot.core.intf.api.cube.IActivePivotManager;
import com.activeviam.mac.cfg.security.impl.SecurityConfig;
import com.activeviam.tech.core.api.agent.AgentException;
import com.activeviam.web.spring.internal.JMXEnabler;
import lombok.RequiredArgsConstructor;
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
      ManagerDescriptionConfig.class,

      // Pivot
      ActivePivotWithDatastoreConfig.class,

      // Content server
      ContentServiceConfig.class,

      // Specific to monitoring server
      SecurityConfig.class,
      SourceConfig.class,
    })
@RequiredArgsConstructor
public class MacServerConfig {

  /** Content Service configuration. */
  private final ContentServiceConfig contentServiceConfig;

  /** Spring configuration of the source files of the Memory Analysis Cube application. */
  private final SourceConfig sourceConfig;

  /**
   * Initialize and start the ActivePivot Manager, after performing all the injections into the
   * ActivePivot plug-ins.
   *
   * @param activePivotManager the ActivePivot Manager of the application
   * @return void
   */
  @Bean
  public Void startManager(final IActivePivotManager activePivotManager) {
    this.contentServiceConfig.loadPredefinedBookmarks();

    /* *********************************************** */
    /* Initialize the ActivePivot Manager and start it */
    /* *********************************************** */
    try {
      activePivotManager.init(null);
      activePivotManager.start();
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
