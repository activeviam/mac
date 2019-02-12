/*
 * (C) Quartet FS 2007-2009
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.server;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import javax.servlet.DispatcherType;

import org.eclipse.jetty.annotations.AnnotationConfiguration;
import org.eclipse.jetty.annotations.ClassInheritanceHandler;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.webapp.Configuration;
import org.eclipse.jetty.webapp.WebAppContext;
import org.springframework.web.WebApplicationInitializer;

import com.activeviam.mac.cfg.impl.MACWebAppInitializer;

/**
 * @author Quartet Financial Systems
 */
public class MacServer {

	/** Root of the web application files, defined relatively to the project root */
	protected static final String WEBAPP = "src/main/webapp";

	/** Jetty server default port (9092) */
	public static final int DEFAULT_PORT = 9092;

	public static Server createServer(int port) {
		// We check that the correct folder is setup as current working directory when starting the Sandbox. Some IDEs like
		// IntelliJ will not use the correct one as default (PIVOT-3106).
		if (!containsFolder(WEBAPP)) {
			System.err.println("The current working directory is incorrect, it should be the parent of the folder " + WEBAPP + ", please change your run configuration.");
			System.exit(1);
		}

		final WebAppContext root = new WebAppContext();
		root.setConfigurations(new Configuration[] { new JettyAnnotationConfiguration() });
		root.setContextPath("/");
		root.setParentLoaderPriority(true);
		root.setResourceBase(WEBAPP);

		// Enable GZIP compression
		final GzipHandler gzipHandler = new GzipHandler();
		gzipHandler.setIncludedMimeTypes(
				"text/html",
				"text/xml",
				"text/javascript",
				"text/css",
				"application/x-java-serialized-object",
				"application/json",
				"application/javascript",
				"image/png",
				"image/svg+xml",
				"image/jpeg");
		gzipHandler.setIncludedMethods(HttpMethod.GET.asString(), HttpMethod.POST.asString());
		gzipHandler.setIncludedPaths("/*");
		gzipHandler.setDispatcherTypes(EnumSet.of(DispatcherType.REQUEST));
		root.setGzipHandler(gzipHandler);

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
		System.setProperty("java.util.logging.config.class", "LoggingConfiguration");

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
		public void preConfigure(WebAppContext context) {
			final Set<String> set = Collections.singleton(MACWebAppInitializer.class.getName());
			final Map<String, Set<String>> map = new ClassInheritanceMap();
			map.put(WebApplicationInitializer.class.getName(), set);
			context.setAttribute(CLASS_INHERITANCE_MAP, map);
			_classInheritanceHandler = new ClassInheritanceHandler(map);
		}

	}

	/**
	 * Returns true if the file with the given name exists.
	 *
	 * @param fileName The path to the file to check for existence.
	 * @return true if the file with the given name exists. False otherwise.
	 */
	static boolean containsFolder(String fileName) {
		return Files.exists(Paths.get(fileName));
	}

}
