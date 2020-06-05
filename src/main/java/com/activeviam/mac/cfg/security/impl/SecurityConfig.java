/*
 * (C) ActiveViam 2019-2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.cfg.security.impl;

import static com.qfs.QfsWebUtils.url;
import static com.qfs.server.cfg.impl.ActivePivotRemotingServicesConfig.ID_GENERATOR_REMOTING_SERVICE;
import static com.qfs.server.cfg.impl.ActivePivotRemotingServicesConfig.LICENSING_REMOTING_SERVICE;
import static com.qfs.server.cfg.impl.ActivePivotRemotingServicesConfig.LONG_POLLING_REMOTING_SERVICE;
import static com.qfs.server.cfg.impl.ActivePivotRestServicesConfig.PING_SUFFIX;
import static com.qfs.server.cfg.impl.ActivePivotRestServicesConfig.REST_API_URL_PREFIX;
import static com.qfs.server.cfg.impl.ActivePivotServicesConfig.ID_GENERATOR_SERVICE;
import static com.qfs.server.cfg.impl.ActivePivotServicesConfig.LICENSING_SERVICE;
import static com.qfs.server.cfg.impl.ActivePivotServicesConfig.LONG_POLLING_SERVICE;
import static com.qfs.server.cfg.impl.CxfServletConfig.CXF_WEB_SERVICES;

import com.qfs.server.cfg.IActivePivotConfig;
import com.quartetfs.biz.pivot.security.impl.UserDetailsServiceWrapper;
import com.quartetfs.fwk.security.IUserDetailsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.servlet.ServletContextInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.BeanIds;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.authentication.switchuser.SwitchUserFilter;

/**
 * Spring configuration fragment for security on an ActivePivot Server.
 *
 * <p>This configuration will in particular load:
 *
 * <ul>
 *   <li>The service to authenticate users
 *   <li>The Spring configuration that defines security on the Version RESTful service
 *   <li>The Spring configuration that defines security on the Content server
 *   <li>The Spring configuration that defines security on the ActivePivot server
 * </ul>
 *
 * @author ActiveViam
 */
@Import(value = {JwtSecurityConfigurer.class, VersionSecurityConfigurer.class})
@Configuration
@EnableWebSecurity
public class SecurityConfig extends ASecurityConfig {

  /** Name of the Cookies of the MAC application. */
  public static final String COOKIE_NAME = "MEMORY_ANALYSIS_CUBE";

  /**
   * Returns the spring security bean user details service wrapper.
   *
   * @return the {@link IUserDetailsService} used as spring security bean user details service
   *     wrapper.
   */
  @Bean
  public IUserDetailsService qfsUserDetailsService() {
    return new UserDetailsServiceWrapper(this.userDetailsConfig.userDetailsService());
  }

  /**
   * To expose the Pivot services.
   *
   * @author ActiveViam
   */
  @Configuration
  public static class ActivePivotSecurityConfigurer extends AWebSecurityConfigurer {

    /** The autowired Spring configuration for ActivePivot. */
    @Autowired protected IActivePivotConfig activePivotConfig;

    /** Constructor. */
    public ActivePivotSecurityConfigurer() {
      super(COOKIE_NAME);
    }

    @Override
    protected void doConfigure(HttpSecurity http) throws Exception {
      http.authorizeRequests()
          // The order of the matchers matters
          .antMatchers(HttpMethod.OPTIONS, REST_API_URL_PREFIX + "/**")
          .permitAll()
          // Web services used by AP live 3.4
          .antMatchers(CXF_WEB_SERVICES + '/' + ID_GENERATOR_SERVICE + "/**")
          .hasAnyAuthority(ROLE_USER, ROLE_TECH)
          .antMatchers(CXF_WEB_SERVICES + '/' + LONG_POLLING_SERVICE + "/**")
          .hasAnyAuthority(ROLE_USER, ROLE_TECH)
          .antMatchers(CXF_WEB_SERVICES + '/' + LICENSING_SERVICE + "/**")
          .hasAnyAuthority(ROLE_USER, ROLE_TECH)
          // Spring remoting services used by AP live 3.4
          .antMatchers(url(ID_GENERATOR_REMOTING_SERVICE, "**"))
          .hasAnyAuthority(ROLE_USER, ROLE_TECH)
          .antMatchers(url(LONG_POLLING_REMOTING_SERVICE, "**"))
          .hasAnyAuthority(ROLE_USER, ROLE_TECH)
          .antMatchers(url(LICENSING_REMOTING_SERVICE, "**"))
          .hasAnyAuthority(ROLE_USER, ROLE_TECH)
          // The ping service is temporarily authenticated (see PIVOT-3149)
          .antMatchers(url(REST_API_URL_PREFIX, PING_SUFFIX))
          .hasAnyAuthority(ROLE_USER, ROLE_TECH)
          // REST services
          .antMatchers(REST_API_URL_PREFIX + "/**")
          .hasAnyAuthority(ROLE_USER)
          // One has to be a user for all the other URLs
          .antMatchers("/**")
          .hasAuthority(ROLE_USER)
          .and()
          .httpBasic()
          // SwitchUserFilter is the last filter in the chain. See FilterComparator class.
          .and()
          .addFilterAfter(activePivotConfig.contextValueFilter(), SwitchUserFilter.class);
    }

    @Bean(name = BeanIds.AUTHENTICATION_MANAGER)
    @Override
    public AuthenticationManager authenticationManagerBean() throws Exception {
      return super.authenticationManagerBean();
    }
  }

  /**
   * Returns a bean initializing the Cookies name in the Servlet Spring context.
   *
   * @return the bean initializing the Cookies name in the Servlet Spring context
   */
  @Bean
  public ServletContextInitializer servletContextInitializer() {
    return servletContext -> servletContext.getSessionCookieConfig().setName(COOKIE_NAME);
  }
}
