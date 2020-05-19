/*
 * (C) ActiveViam 2015-2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.cfg.impl;

import com.qfs.content.cfg.impl.ContentServerRestServicesConfig;
import com.qfs.content.service.IContentService;
import com.qfs.content.service.audit.impl.AuditableHibernateContentService;
import com.qfs.content.service.impl.HibernateContentService;
import com.qfs.pivot.content.IActivePivotContentService;
import com.qfs.pivot.content.impl.ActivePivotContentServiceBuilder;
import com.qfs.server.cfg.content.IActivePivotContentServiceConfig;
import com.quartetfs.biz.pivot.context.IContextValue;
import com.quartetfs.biz.pivot.definitions.ICalculatedMemberDescription;
import com.quartetfs.biz.pivot.definitions.IKpiDescription;
import java.lang.reflect.Method;
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
 * @author Quartet FS
 */
@Configuration
public class LocalContentServiceConfig implements IActivePivotContentServiceConfig {

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
	 * Instance of the Spring context environment.
	 */
	@Autowired
	public Environment env;

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
		return new AuditableHibernateContentService(conf);
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
						env.getRequiredProperty(CALCULATED_MEMBER_ROLE_PROPERTY),
						env.getRequiredProperty(KPI_ROLE_PROPERTY))
				.build();
	}

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
	 *                            file.
	 * @return the {@link DataSource} for {@link HibernateContentService}.
	 */
	private static DataSource createTomcatJdbcDataSource(Properties hibernateProperties) {
		try {
			// Reflection is used to not make the sandbox depends on tomcat-jdbc.jar
			Class<?> dataSourceKlass = Class.forName("org.apache.tomcat.jdbc.pool.DataSourceFactory");
			Method createDataSourceMethod =
					dataSourceKlass.getMethod("createDataSource", Properties.class);
			return (DataSource)
					createDataSourceMethod.invoke(dataSourceKlass.newInstance(), hibernateProperties);
		} catch (Exception e) {
			throw new BeanInitializationException("Initialization of " + DataSource.class + " failed", e);
		}
	}
}
