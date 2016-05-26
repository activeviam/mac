package com.qfs.monitoring.cfg.impl;

import java.util.Set;

import com.qfs.server.cfg.impl.ASpringResourceServerConfig;
import com.qfs.util.impl.QfsArrays;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LiveResourceServerConfig extends ASpringResourceServerConfig {

	public LiveResourceServerConfig() {
		super("/live");
	}

	/**
	 * Gets the extensions of files to serve.
	 * @return all files extensions
	 */
	@Override
	public Set<String> getServedExtensions() {
		return QfsArrays.mutableSet(
				// Default html files
				"html", "js", "css", "map", "json",
				// Image extensions
				"png", "jpg", "gif", "ico",
				// Font extensions
				"eot", "svg", "ttf", "woff", "woff2"
		);
	}

	@Override
	public Set<String> getServedDirectories() {
		return QfsArrays.mutableSet("/");
	}

	@Override
	public Set<String> getResourceLocations() {
		return QfsArrays.mutableSet("/live/");
	}

}
