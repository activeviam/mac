/*
 * (C) Quartet FS 2007-2009
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.qfs.sandbox.server;

import java.util.EnumSet;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.DispatcherType;

import org.eclipse.jetty.annotations.AnnotationConfiguration;
import org.eclipse.jetty.annotations.ClassInheritanceHandler;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.eclipse.jetty.webapp.Configuration;
import org.eclipse.jetty.webapp.WebAppContext;
import org.springframework.web.WebApplicationInitializer;

import com.qfs.monitoring.cfg.impl.WebAppInitializer;

/**
 * @author Quartet Financial Systems
 */
public class MonitoringServer {

	/** Jetty server default port (9090) */
	public static final int DEFAULT_PORT = 9092;

	public static Server createServer(int port) {
		final WebAppContext root = new WebAppContext();
		root.setConfigurations(new Configuration[] { new JettyAnnotationConfiguration() });
		root.setContextPath("/");
		root.setParentLoaderPriority(true);
		root.setResourceBase("src/main/webapp");

		// Enable GZIP compression
		final FilterHolder gzipFilter = new FilterHolder(org.eclipse.jetty.servlets.GzipFilter.class);
		gzipFilter.setInitParameter(
				"mimeTypes",
				"text/html,"
						+ "text/xml,"
						+ "text/javascript,"
						+ "text/css,"
						+ "application/x-java-serialized-object,"
						+ "application/json,"
						+ "application/javascript,"
						+ "image/png,"
						+ "image/svg+xml"
						+ "image/jpeg");
		gzipFilter.setInitParameter("methods", HttpMethod.GET.asString() + "," + HttpMethod.POST.asString());
		root.addFilter(gzipFilter, "/*", EnumSet.of(DispatcherType.REQUEST));

		// Create server and configure it
		final Server server = new Server(port);
		server.setHandler(root);

		return server;
	}

	/**
	 * Configure and launch the standalone server.
	 * @param args only one optional argument is supported: the server port
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception {

		//Set default logging
		System.setProperty("java.util.logging.config.class", "com.qfs.logging.LoggingConfiguration");

		int port = DEFAULT_PORT;
		if (args != null && args.length >= 1) {
			port = Integer.parseInt(args[0]);
		}

		final Server server = createServer(port);

		// Launch the server
		server.start();
		server.join();
	}

	/**
	 *
	 * When the Jetty servlet-3.0 annotation parser is used, it only
	 * scans the jar files in the classpath. This small override will
	 * allow Jetty to also see the Sandbox web application initializer
	 * in the classpath of the IDE (Eclipse for instance).
	 *
	 * @author Quartet FS
	 *
	 */
	public static class JettyAnnotationConfiguration extends AnnotationConfiguration {

		@Override
		public void preConfigure(WebAppContext context) throws Exception {
			ConcurrentHashSet<String> set = new ConcurrentHashSet<>();
			ConcurrentHashMap<String, ConcurrentHashSet<String>> map = new ClassInheritanceMap();
			set.add(WebAppInitializer.class.getName());
			map.put(WebApplicationInitializer.class.getName(), set);
			context.setAttribute(CLASS_INHERITANCE_MAP, map);
			_classInheritanceHandler = new ClassInheritanceHandler(map);
		}

	}

}
