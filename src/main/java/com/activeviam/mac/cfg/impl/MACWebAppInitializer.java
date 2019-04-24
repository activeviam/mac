/*
 * (C) ActiveViam 2013-2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.cfg.impl;

import static org.springframework.security.config.BeanIds.SPRING_SECURITY_FILTER_CHAIN;

import java.util.EnumSet;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRegistration.Dynamic;

import com.activeviam.mac.cfg.security.impl.SecurityConfig;
import org.springframework.web.WebApplicationInitializer;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.filter.DelegatingFilterProxy;
import org.springframework.web.servlet.DispatcherServlet;

/**
 *
 * Initializer of the Web Application.
 * <p>
 * When bootstrapped by a servlet-3.0 application container, the Spring
 * Framework will automatically create an instance of this class and call its
 * startup callback method.
 * <p>
 * The content of this class replaces the old web.xml file in previous versions
 * of the servlet specification.
 *
 * @author Quartet FS
 *
 */
public class MACWebAppInitializer implements WebApplicationInitializer {

	@Override
	public void onStartup(ServletContext servletContext) throws ServletException {
		// Spring Context Bootstrapping
		AnnotationConfigWebApplicationContext rootAppContext = new AnnotationConfigWebApplicationContext();
		rootAppContext.register(MacServerConfig.class);
		servletContext.addListener(new ContextLoaderListener(rootAppContext));
		// Change the session cookie name. Needs only to be done when there are several servers (AP,
		// Content server, Sentinel) with the same URL but running on different ports.
		// Cookies ignore the port (See RFC 6265).
		servletContext.getSessionCookieConfig().setName(SecurityConfig.COOKIE_NAME);

		// The main servlet/the central dispatcher
		final DispatcherServlet servlet = new DispatcherServlet(rootAppContext);
		servlet.setDispatchOptionsRequest(true);
		Dynamic dispatcher = servletContext.addServlet("springDispatcherServlet", servlet);
		dispatcher.addMapping("/*");
		dispatcher.setLoadOnStartup(1);

		// Spring Security Filter
		final FilterRegistration.Dynamic springSecurity = servletContext
				.addFilter(SPRING_SECURITY_FILTER_CHAIN, new DelegatingFilterProxy());
		springSecurity.addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), true, "/*");
	}

}
