/*
 * (C) Quartet FS 2015
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.qfs.monitoring.cfg.impl;

import java.util.Arrays;
import java.util.List;

import org.springframework.context.annotation.Configuration;

import com.qfs.security.cfg.impl.ACorsFilterConfig;

/**
 * @author Quartet FS
 */
@Configuration
public class MonitoringCorsFilterConfig extends ACorsFilterConfig {

	@Override
	public List<String> getAllowedOrigins() {
		// Should not be empty in production. Empty means allow all origins.
		// You should put the url(s) of JavaScript code which must access to ActivePivot REST services
		return Arrays.asList();
	}

}
