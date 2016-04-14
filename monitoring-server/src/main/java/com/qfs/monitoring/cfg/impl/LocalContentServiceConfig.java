/*
 * (C) Quartet FS 2015-2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.qfs.monitoring.cfg.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;

import com.qfs.content.cfg.impl.ContentServerRestServicesConfig;
import com.qfs.content.service.IContentService;
import com.qfs.pivot.content.IActivePivotContentService;
import com.qfs.pivot.content.impl.ActivePivotContentServiceBuilder;
import com.qfs.server.cfg.IActivePivotContentServiceConfig;
import com.qfs.server.cfg.impl.JwtRestServiceConfig;
import com.quartetfs.biz.pivot.context.IContextValue;
import com.quartetfs.biz.pivot.definitions.ICalculatedMemberDescription;
import com.quartetfs.biz.pivot.definitions.IKpiDescription;

/**
 * Spring configuration of the Content Service.
 * <p>
 * This configuration does not rely on an external server.
 * <p>
 * This configuration imports {@link ContentServerRestServicesConfig} to expose the content service.
 *
 * @author Quartet FS
 */
@Import({ ContentServerRestServicesConfig.class, JwtRestServiceConfig.class })
@Configuration
public class LocalContentServiceConfig implements IActivePivotContentServiceConfig {

	/**
	 * The name of the property which contains the role allowed to add new calculated members in the configuration service.
	 */
	public static final String CALCULATED_MEMBER_ROLE_PROPERTY = "contentServer.security.calculatedMemberRole";

	/**
	 * The name of the property which contains the role allowed to add new KPIs in the configuration service.
	 */
	public static final String KPI_ROLE_PROPERTY = "contentServer.security.kpiRole";

	public static final String ENTITLEMENTS_TTL = "contentServer.security.cache.entitlementsTTL";

	@Autowired
	public Environment env;

	/**
	 * Service used to store the ActivePivot descriptions and the entitlements (i.e.
	 * {@link IContextValue context values}, {@link ICalculatedMemberDescription calculated members}
	 * and {@link IKpiDescription KPIs}).
	 *
	 * @return the {@link IActivePivotContentService content service} used by the Sandbox
	 *         application
	 */
	@Bean
	@Override
	public IActivePivotContentService activePivotContentService() {
		return new ActivePivotContentServiceBuilder()
				.withoutPersistence()
				.withCacheForEntitlements(-1)

				// WARNING: In production, you should not keep the next lines, which will erase parts
				// of your remote configuration. Prefer pushing them manually using the PushToContentServer utility class.

				// Setup directories and permissions
				.needInitialization(env.getRequiredProperty(CALCULATED_MEMBER_ROLE_PROPERTY),
						env.getRequiredProperty(KPI_ROLE_PROPERTY))
				// Push the manager description from DESC-INF
				.withXmlDescription()
				// Push the context values stored in ROLE-INF
				.build();
	}

	@Bean
	@Override
	public IContentService contentService() {
		// Return the real content service used by the activePivotContentService instead of the wrapped one
		return activePivotContentService().getContentService().getUnderlying();
	}

}
