/*
 * (C) Quartet FS 2007-2017
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.qfs.monitoring.cfg.impl;

import java.io.IOException;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.qfs.server.cfg.IActivePivotManagerConfig;
import com.qfs.util.impl.QfsFiles;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.impl.AResourceDeserializer;
import com.quartetfs.biz.pivot.definitions.impl.DescriptionUtil;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.serialization.IJaxbSerializer;
import com.quartetfs.fwk.serialization.ISerializer;

/**
 * @author ActiveViam
 *
 */
@Configuration
public class ActivePivotManagerConfig implements IActivePivotManagerConfig {

	@Override
	@Bean
	public IActivePivotManagerDescription managerDescription() {
		return DescriptionUtil
				.<IActivePivotManagerDescription> resolve(
						"DESC-INF/ActivePivotManager.xml",
						new AResourceDeserializer(getSerializer()) {
			@Override
			public String resolveResource(final String resourceName) {
				return getDescriptionAsString(resourceName);
			}
		});
	}

	public String getDescriptionAsString(final String resourceName) {
		try {
			return QfsFiles.readFile(resourceName);
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	protected ISerializer getSerializer() {
		return Registry.create(IJaxbSerializer.class);
	}

}
