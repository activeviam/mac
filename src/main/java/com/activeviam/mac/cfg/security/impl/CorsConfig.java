/*
 * (C) Quartet FS 2015
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.cfg.security.impl;

import java.util.Arrays;
import java.util.List;

import org.springframework.context.annotation.Configuration;

import com.qfs.security.cfg.impl.ACorsFilterConfig;

/**
 * Spring configuration for CORS filter for the Sandbox.
 * <p>
 * The Sandbox configuration is to this regard not a production configuration,
 * see {@link #getAllowedOrigins()} to correct it for a production environment.
 *
 * @author Quartet FS
 */
@Configuration
public class CorsConfig extends ACorsFilterConfig {

	@Override
	public List<String> getAllowedOrigins() {
		// You should put here the urls(s) from which web applications will make HTTP requests to this server.
		// This is needed at least for ActiveViam REST services consumed by ActiveUI.
		// An empty list means allowing all origins.
		// Allowing all origins is not recommended in a production environment since it allows any web application
		// to which users have access to make any HTTP call on this server with the credentials of the user.
		// The default implementation is a development configuration and allows all origins to ease use and testing.
		return Arrays.asList();
	}
}
