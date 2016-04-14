/*
 * (C) Quartet FS 2012-2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.qfs.monitoring.cfg.impl;

import static com.qfs.server.cfg.impl.ActivePivotRemotingServicesConfig.ID_GENERATOR_REMOTING_SERVICE;
import static com.qfs.server.cfg.impl.ActivePivotRemotingServicesConfig.LICENSING_REMOTING_SERVICE;
import static com.qfs.server.cfg.impl.ActivePivotRemotingServicesConfig.LONG_POLLING_REMOTING_SERVICE;
import static com.qfs.server.cfg.impl.ActivePivotRestServicesConfig.REST_API_URL_PREFIX;
import static com.qfs.server.cfg.impl.ActivePivotServicesConfig.ID_GENERATOR_SERVICE;
import static com.qfs.server.cfg.impl.ActivePivotServicesConfig.LICENSING_SERVICE;
import static com.qfs.server.cfg.impl.ActivePivotServicesConfig.LONG_POLLING_SERVICE;
import static com.qfs.server.cfg.impl.CxfServletConfig.CXF_WEB_SERVICES;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.BeanIds;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.authentication.switchuser.SwitchUserFilter;

import com.qfs.content.cfg.impl.ContentServerRestServicesConfig;
import com.qfs.pivot.servlet.impl.ContextValueFilter;
import com.qfs.server.cfg.IActivePivotConfig;
import com.quartetfs.biz.pivot.security.IUserDetailsService;
import com.quartetfs.biz.pivot.security.impl.UserDetailsServiceWrapper;

/**
 * Spring configuration fragment for security.
 *
 * @author Quartet FS
 */
@Configuration
@EnableWebSecurity
public class SecurityConfig extends ASecurityConfig {

	public static final String COOKIE_NAME = "AP_JSESSIONID";

	/**
	 * User details service wrapped into a Quartet interface.
	 * <p>
	 * This bean is used by {@link SentinelAPExtensionServiceConfiguration}
	 *
	 * @return a user details service
	 */
	@Bean
	public IUserDetailsService qfsUserDetailsService() {
		return new UserDetailsServiceWrapper(userDetailsService());
	}

	/**
	 * Only required if the content service is exposed.
	 * <p>
	 * Separated from {@link ActivePivotSecurityConfigurer} to skip the {@link ContextValueFilter}.
	 */
	@Configuration
	@Order(1)
	// Must be done before ActivePivotSecurityConfigurer (because they match common URLs)
	public static class JwtSecurityConfigurer extends AJwtSecurityConfigurer {}

	/**
	 * Only required if the content service is exposed.
	 * <p>
	 * Separated from {@link ActivePivotSecurityConfigurer} to skip the {@link ContextValueFilter}.
	 *
	 * @see LocalContentServiceConfig
	 */
	@Configuration
	@Order(2)
	// Must be done before ActivePivotSecurityConfigurer (because they match common URLs)
	public static class ContentServerSecurityConfigurer extends AWebSecurityConfigurer {

		@Override
		protected void doConfigure(HttpSecurity http) throws Exception {
			final String url = ContentServerRestServicesConfig.NAMESPACE;
			http
					// Only theses URLs must by handled by this HttpSecurity
					.antMatcher(url + "/**")
					.authorizeRequests()
					// The order of the matchers matters
					.antMatchers(
							HttpMethod.OPTIONS,
							ContentServerRestServicesConfig.REST_API_URL_PREFIX
									+ "/**").permitAll()
					.antMatchers(url + "/**").hasAuthority(ROLE_USER)
					.and().httpBasic();
		}

	}

	@Configuration
	public static class ActivePivotSecurityConfigurer extends AWebSecurityConfigurer {

		@Autowired
		protected IActivePivotConfig activePivotConfig;

		public ActivePivotSecurityConfigurer() {
			super(COOKIE_NAME);
		}

		@Override
		protected void doConfigure(HttpSecurity http) throws Exception {
			http
					.authorizeRequests()
					// The order of the matchers matters
					.antMatchers(HttpMethod.OPTIONS, REST_API_URL_PREFIX + "/**").permitAll()
					// Web services used by AP live
					.antMatchers(CXF_WEB_SERVICES + '/' + ID_GENERATOR_SERVICE + "/**").hasAnyAuthority(ROLE_USER, ROLE_TECH)
					.antMatchers(CXF_WEB_SERVICES + '/' + LONG_POLLING_SERVICE + "/**").hasAnyAuthority(ROLE_USER, ROLE_TECH)
					.antMatchers(CXF_WEB_SERVICES + '/' + LICENSING_SERVICE + "/**").hasAnyAuthority(ROLE_USER, ROLE_TECH)
					// Spring remoting services used by AP live
					.antMatchers(ID_GENERATOR_REMOTING_SERVICE + "/**").hasAnyAuthority(ROLE_USER, ROLE_TECH)
					.antMatchers(LONG_POLLING_REMOTING_SERVICE + "/**").hasAnyAuthority(ROLE_USER, ROLE_TECH)
					.antMatchers(LICENSING_REMOTING_SERVICE + "/**").hasAnyAuthority(ROLE_USER, ROLE_TECH)
					// One has to be a user for all the other URLs
					.antMatchers("/**").hasAuthority(ROLE_USER)
					.and().httpBasic()
					// SwitchUserFilter is the last filter in the chain. See FilterComparator class.
					.and().addFilterAfter(activePivotConfig.contextValueFilter(), SwitchUserFilter.class);
		}

		@Bean(name = BeanIds.AUTHENTICATION_MANAGER)
		@Override
		public AuthenticationManager authenticationManagerBean() throws Exception {
			return super.authenticationManagerBean();
		}

	}

}
