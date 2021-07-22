/*
 * (C) ActiveViam 2017
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.cfg.security.impl;

import static com.activeviam.mac.cfg.security.impl.ASecurityConfig.ACTIVEUI_ADDRESS;

import com.activeviam.mac.cfg.security.impl.ASecurityConfig.AWebSecurityConfigurer;
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
public class ActiveUiSecurityConfigurer extends AWebSecurityConfigurer {

  @Override
  protected void doConfigure(HttpSecurity http) throws Exception {
    final String activeUiUrl = env.getRequiredProperty(ACTIVEUI_ADDRESS);
    http
        // Only theses URLs must be handled by this HttpSecurity
        .regexMatcher(activeUiUrl)
        .authorizeRequests()
        // The order of the matchers matters
        .regexMatchers(HttpMethod.OPTIONS, activeUiUrl)
        .permitAll()
        .regexMatchers(HttpMethod.GET, activeUiUrl)
        .permitAll();
    //this allows pre-flight cross-origin requests
    http.cors();
    // Authorizing pages to be embedded in iframes to have ActiveUI in ActiveMonitor UI
    http.headers().frameOptions().disable();
  }
}
