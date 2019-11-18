/*
 * (C) ActiveViam 2017
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.cfg.security.impl;

import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;

/**
 * To expose the login page of ActiveUI.
 *
 * @author ActiveViam
 */
@Configuration
@Order(1)
public class ActiveUISecurityConfigurer extends ASecurityConfig.AWebSecurityConfigurer {

  @Override
  protected void doConfigure(HttpSecurity http) throws Exception {
    // Permit all on ActiveUI resources and the root (/) that redirects to ActiveUI index.html.
    final String pattern = "^(.{0}|\\/|\\/" + "ui" + "(\\/.*)?)$";
    http
        // Only theses URLs must be handled by this HttpSecurity
        .regexMatcher(pattern)
        .authorizeRequests()
        // The order of the matchers matters
        .regexMatchers(HttpMethod.OPTIONS, pattern)
        .permitAll()
        .regexMatchers(HttpMethod.GET, pattern)
        .permitAll();

    // Authorizing pages to be embedded in iframes to have ActiveUI in ActiveMonitor UI
    http.headers().frameOptions().disable();
  }
}
