/*
 * (C) ActiveViam 2017
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.cfg.security.impl;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.core.userdetails.UserDetailsService;

/**
 * Spring configuration that defines the users and their associated roles in the Sandbox
 * application.
 *
 * @author ActiveViam
 */
@Configuration
public class UserConfig {

  /**
   * ROLE_KPI is added to users, to give them permission to read kpis created by other users in the
   * content server In order to "share" kpis created in the content server, the kpi reader role is
   * set to : ROLE_KPI
   */
  public static final String ROLE_KPI = "ROLE_KPI";

  /**
   * [Bean] Create the users that can access the application
   *
   * @return {@link UserDetailsService user data}
   */
  @Bean
  public UserDetailsService userDetailsService() {
    return new InMemoryUserDetailsManagerBuilder()

        // "Real users"
        .withUser("admin")
        .password("admin")
        .authorities(
            SecurityConfig.ROLE_USER,
            SecurityConfig.ROLE_ADMIN,
            ROLE_KPI,
            SecurityConfig.ROLE_CS_ROOT)
        .and()

        // We're done
        .build();
  }
}
