/*
 * (C) ActiveViam 2017
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.cfg.security.impl;

import com.qfs.server.cfg.impl.JwtRestServiceConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.web.AuthenticationEntryPoint;

/**
 * To expose the JWT REST service.
 *
 * @author ActiveViam
 */
@Configuration
@Order(2) // Must be done before ContentServerSecurityConfigurer (because they match common URLs)
public class JwtSecurityConfigurer extends WebSecurityConfigurerAdapter {

  /** The autowired Spring @link {@link ApplicationContext}. */
  @Autowired protected ApplicationContext context;

  @Override
  protected void configure(HttpSecurity http) throws Exception {
    final AuthenticationEntryPoint basicAuthenticationEntryPoint =
        this.context.getBean(ASecurityConfig.BASIC_AUTH_BEAN_NAME, AuthenticationEntryPoint.class);
    http.antMatcher(JwtRestServiceConfig.REST_API_URL_PREFIX + "/**")
        // As of Spring Security 4.0, CSRF protection is enabled by default.
        .csrf()
        .disable()
        .cors()
        .and()
        // Configure CORS
        .authorizeRequests()
        .antMatchers(HttpMethod.OPTIONS, "/**")
        .permitAll()
        .antMatchers("/**")
        .hasAnyAuthority(ASecurityConfig.ROLE_USER)
        .and()
        .httpBasic()
        .authenticationEntryPoint(basicAuthenticationEntryPoint);
  }
}
