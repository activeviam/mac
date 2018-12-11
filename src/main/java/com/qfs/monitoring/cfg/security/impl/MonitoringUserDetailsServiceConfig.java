/*
 * (C) Quartet FS 2017
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.qfs.monitoring.cfg.security.impl;

import static com.qfs.monitoring.cfg.security.impl.ASecurityConfig.ROLE_ADMIN;
import static com.qfs.monitoring.cfg.security.impl.ASecurityConfig.ROLE_CS_ROOT;
import static com.qfs.monitoring.cfg.security.impl.ASecurityConfig.ROLE_TECH;
import static com.qfs.monitoring.cfg.security.impl.ASecurityConfig.ROLE_USER;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.core.userdetails.UserDetailsService;


/**
 * Spring configuration that defines the users and their associated
 * roles in the Sandbox application.
 *
 * @author ActiveViam
 */
@Configuration
public class MonitoringUserDetailsServiceConfig {

	/**
	 * ROLE_KPI is added to users, to give them permission
	 * to read kpis created by other users in the content server
	 * In order to "share" kpis created in the content server, the kpi
	 * reader role is set to : ROLE_KPI
	 */
	public static final String ROLE_KPI = "ROLE_KPI";

	public static final String PIVOT_USER = "pivot";
	public static final String[] PIVOT_USER_ROLES = { ROLE_TECH, ROLE_CS_ROOT };


	/**
	 * [Bean] Create the users that can access the application
	 *
	 * @return {@link UserDetailsService user data}
	 */
	@Bean
	public UserDetailsService userDetailsService() {
		return new InMemoryUserDetailsManagerBuilder()

				// "Real users"
				.withUser("admin").password("admin").authorities(ROLE_USER, ROLE_ADMIN, ROLE_KPI, ROLE_CS_ROOT).and()
				.withUser("user1").password("user1").authorities(ROLE_USER, ROLE_KPI, "ROLE_DESK_A").and()
				.withUser("user2").password("user2").authorities(ROLE_USER, "ROLE_EUR_USD").and()
				.withUser("manager1").password("manager1").authorities(ROLE_USER, ROLE_KPI).and()
				.withUser("manager2").password("manager2").authorities(ROLE_USER, ROLE_KPI).and()

				// Technical user for ActivePivot Live access
				.withUser("live").password("live").authorities(ROLE_TECH).and()
				// Technical user for ActivePivot server communication with the content server.
				.withUser(PIVOT_USER).password("pivot").authorities(PIVOT_USER_ROLES).and()

				// We're done
				.build();
	}


}
