/*
 * (C) ActiveViam 2015-2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.cfg.impl;

import static com.activeviam.tech.contentserver.storage.api.ContentServiceSnapshotter.create;

import com.activeviam.activepivot.core.intf.api.contextvalues.IContextValue;
import com.activeviam.activepivot.core.intf.api.description.ICalculatedMemberDescription;
import com.activeviam.activepivot.core.intf.api.description.IKpiDescription;
import com.activeviam.activepivot.server.intf.api.entitlements.IActivePivotContentService;
import com.activeviam.activepivot.server.spring.api.config.IActivePivotContentServiceConfig;
import com.activeviam.activepivot.server.spring.api.content.ActivePivotContentServiceBuilder;
import com.activeviam.mac.cfg.security.impl.SecurityConfig;
import com.activeviam.tech.contentserver.spring.internal.config.ContentServerRestServicesConfig;
import com.activeviam.tech.contentserver.storage.api.IContentService;
import com.activeviam.tech.contentserver.storage.private_.HibernateContentService;
import com.activeviam.tech.core.internal.monitoring.JmxOperation;
import com.activeviam.tools.bookmark.constant.impl.ContentServerConstants.Paths;
import com.activeviam.tools.bookmark.constant.impl.ContentServerConstants.Role;
import com.activeviam.tools.bookmark.impl.BookmarkTool;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import org.hibernate.HibernateException;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.AvailableSettings;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

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
@RequiredArgsConstructor
public class ContentServiceConfig implements IActivePivotContentServiceConfig {

  /**
   * The name of the property that controls whether or not to force the reloading of the predefined
   * bookmarks even if they were already loaded previously.
   */
  public static final String FORCE_BOOKMARK_RELOAD_PROPERTY = "bookmarks.reloadOnStartup";

  /** The name of the property that precise the name of the folder the bookmarks are in. */
  public static final String UI_FOLDER_PROPERTY = "bookmarks.folder";

  private final Environment env;

  /**
   * Loads the Hibernate's configuration from the specified file.
   *
   * @return the Hibernate's configuration
   */
  private static SessionFactory loadConfiguration(final Properties hibernateProperties)
      throws HibernateException, IOException {
    hibernateProperties.put(
        AvailableSettings.DATASOURCE, createTomcatJdbcDataSource(hibernateProperties));
    final Resource entityMappingFile = new ClassPathResource("content-service-hibernate.xml");
    return new org.hibernate.cfg.Configuration()
        .addProperties(hibernateProperties)
        .addInputStream(entityMappingFile.getInputStream())
        .buildSessionFactory();
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
    if ("db".equals(this.env.getProperty("content-service.type", "db"))) {
      return IContentService.builder().inMemory().build();
    } else {
      final SessionFactory sessionFactory;
      try {
        sessionFactory = loadConfiguration(contentServiceHibernateProperties());
        return new HibernateContentService(sessionFactory);
      } catch (HibernateException | IOException e) {
        throw new BeanInitializationException("Failed to initialize the Content Service", e);
      }
    }
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
        .needInitialization(SecurityConfig.ROLE_USER, SecurityConfig.ROLE_USER)
        .build();
  }

  private Map<String, List<String>> defaultBookmarkPermissions() {
    return Map.of(
        Role.OWNERS,
        List.of(SecurityConfig.ROLE_USER),
        Role.READERS,
        List.of(SecurityConfig.ROLE_USER));
  }

  /**
   * Exports the bookmarks from the Content Service.
   *
   * <p>This is used to back up the defined bookmarks to load them at boot time.
   */
  @JmxOperation(
      name = "exportBookMarks",
      desc = "Export the current bookmark structure",
      params = {"destination"})
  public void exportBookMarks(String destination) {
    BookmarkTool.exportBookmarks(create(contentService().withRootPrivileges()), destination);
  }

  /** Loads the bookmarks packaged with the application. */
  public void loadPredefinedBookmarks() {
    final var service = contentService().withRootPrivileges();
    if (!service.exists("/" + Paths.UI) || shouldReloadBookmarks()) {
      BookmarkTool.importBookmarks(create(service), defaultBookmarkPermissions());
    }
  }

  /** Returns true if the bookmarks must be reloaded even if already present. */
  private boolean shouldReloadBookmarks() {
    return this.env.getProperty(FORCE_BOOKMARK_RELOAD_PROPERTY, Boolean.class, false);
  }
}
