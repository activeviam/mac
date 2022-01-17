/*
 * (C) ActiveViam 2015-2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.cfg.impl;

import com.activeviam.mac.cfg.security.impl.SecurityConfig;
import com.activeviam.tools.bookmark.constant.impl.ContentServerConstants.Role;
import com.activeviam.tools.bookmark.impl.BookmarkTool;
import com.qfs.content.cfg.impl.ContentServerRestServicesConfig;
import com.qfs.content.service.IContentService;
import com.qfs.content.service.impl.HibernateContentService;
import com.qfs.content.snapshot.impl.ContentServiceSnapshotter;
import com.qfs.jmx.JmxOperation;
import com.qfs.pivot.content.IActivePivotContentService;
import com.qfs.pivot.content.impl.ActivePivotContentServiceBuilder;
import com.qfs.server.cfg.content.IActivePivotContentServiceConfig;
import com.quartetfs.biz.pivot.context.IContextValue;
import com.quartetfs.biz.pivot.definitions.ICalculatedMemberDescription;
import com.quartetfs.biz.pivot.definitions.IKpiDescription;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.sql.DataSource;
import org.hibernate.cfg.AvailableSettings;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/**
 * Spring configuration of the Content Service.
 *
 * <p>This configuration does not rely on an external server.
 *
 * <p>This configuration imports {@link ContentServerRestServicesConfig} to expose the content
 * service.
 *
 * @author ActiveViam
 */
@Configuration
public class ContentServiceConfig implements IActivePivotContentServiceConfig {

  /**
   * The name of the property which contains the role allowed to add new calculated members in the
   * configuration service.
   */
  public static final String CALCULATED_MEMBER_ROLE_PROPERTY =
      "contentServer.security.calculatedMemberRole";

  /**
   * The name of the property which contains the role allowed to add new KPIs in the configuration
   * service.
   */
  public static final String KPI_ROLE_PROPERTY = "contentServer.security.kpiRole";

  /**
   * The name of the property that controls whether or not to force the reloading of the predefined
   * bookmarks even if they were already loaded previously.
   */
  public static final String FORCE_BOOKMARK_RELOAD_PROPERTY = "bookmarks.reloadOnStartup";

  /** Instance of the Spring context environment. */
  @Autowired public Environment env;

  /**
   * Loads the Hibernate's configuration from the specified file.
   *
   * @return the Hibernate's configuration
   */
  private static org.hibernate.cfg.Configuration loadConfiguration(
      final Properties hibernateProperties) {
    hibernateProperties.put(
        AvailableSettings.DATASOURCE, createTomcatJdbcDataSource(hibernateProperties));
    return new org.hibernate.cfg.Configuration().addProperties(hibernateProperties);
  }

  /**
   * This {@link DataSource} is specific to the connection pool we want to use with Hibernate. If
   * you don't want to use the same as we do, you don't need it.
   *
   * @param hibernateProperties the hibernate properties loaded from <i>hibernate.properties</i>
   *     file.
   * @return the {@link DataSource} for {@link HibernateContentService}.
   */
  private static DataSource createTomcatJdbcDataSource(Properties hibernateProperties) {
    try {
      // Reflection is used to not make the sandbox depends on tomcat-jdbc.jar
      Class<?> dataSourceKlass = Class.forName("org.apache.tomcat.jdbc.pool.DataSourceFactory");
      Method createDataSourceMethod =
          dataSourceKlass.getMethod("createDataSource", Properties.class);
      return (DataSource)
          createDataSourceMethod.invoke(
              dataSourceKlass.getDeclaredConstructor().newInstance(), hibernateProperties);
    } catch (Exception e) {
      throw new BeanInitializationException("Initialization of " + DataSource.class + " failed", e);
    }
  }

  /**
   * [Bean] Configuration for the Content Service database.
   *
   * @return configuration properties
   */
  @ConfigurationProperties(prefix = "content-service.db")
  @Bean
  public Properties contentServiceHibernateProperties() {
    return new Properties();
  }

  /**
   * The content service is a bean which can be used by ActivePivot server to store.
   *
   * <ul>
   *   <li>calculated members and share them between users
   *   <li>the cube descriptions
   *   <li>entitlements
   * </ul>
   *
   * @return the content service
   */
  @Override
  @Bean
  public IContentService contentService() {
    org.hibernate.cfg.Configuration conf = loadConfiguration(contentServiceHibernateProperties());
    return new HibernateContentService(conf);
  }

  /**
   * Service used to store the ActivePivot descriptions and the entitlements (i.e. {@link
   * IContextValue context values}, {@link ICalculatedMemberDescription calculated members} and
   * {@link IKpiDescription KPIs}).
   *
   * @return the {@link IActivePivotContentService content service} used by the Sandbox application
   */
  @Bean
  @Override
  public IActivePivotContentService activePivotContentService() {
    return new ActivePivotContentServiceBuilder()
        .with(contentService())
        .withCacheForEntitlements(-1)
        .needInitialization(
            this.env.getRequiredProperty(CALCULATED_MEMBER_ROLE_PROPERTY),
            this.env.getRequiredProperty(KPI_ROLE_PROPERTY))
        .build();
  }

  private Map<String, List<String>> defaultBookmarkPermissions() {
    return Map.of(
        Role.OWNERS, List.of(SecurityConfig.ROLE_CS_ROOT),
        Role.READERS, List.of(SecurityConfig.ROLE_CS_ROOT));
  }

  /**
   * Exports the bookmarks from the Content Service.
   *
   * <p>This is used to back up the defined bookmarks to load them at boot time.
   */
  @JmxOperation(
      name = "exportBookMarks",
      desc = "Export the current bookmark structure",
      params = {})
  @SuppressWarnings("unused")
  public void exportBookMarks() {
    BookmarkTool.exportBookmarks(
        new ContentServiceSnapshotter(contentService().withRootPrivileges()),
        "bookmark-export",
        defaultBookmarkPermissions());
  }

  /** Loads the bookmarks packaged with the application. */
  public void loadPredefinedBookmarks() {
    final var service = contentService().withRootPrivileges();
    if (!service.exists("/ui/bookmarks") || shouldReloadBookmarks()) {
      BookmarkTool.importBookmarks(
          new ContentServiceSnapshotter(service), "bookmarks", defaultBookmarkPermissions());
    }
  }

  /** Returns true if the bookmarks must be reloaded even if already present. */
  private boolean shouldReloadBookmarks() {
    return this.env.getProperty(FORCE_BOOKMARK_RELOAD_PROPERTY, Boolean.class, false);
  }
}
