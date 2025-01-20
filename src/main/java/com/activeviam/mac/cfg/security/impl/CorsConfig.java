/*
 * (C) ActiveViam 2017-2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.cfg.security.impl;

import com.activeviam.web.spring.api.config.ICorsConfig;
import java.util.Collections;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.security.config.annotation.authentication.configuration.EnableGlobalAuthentication;
import org.springframework.web.cors.CorsConfiguration;

/**
 * Generic implementation for security configuration of a server hosting ActivePivot, or Content
 * server or ActiveMonitor.
 *
 * <p>This class contains methods:
 *
 * <ul>
 *   <li>To define authorized users,
 *   <li>To enable anonymous user access,
 *   <li>To configure the JWT filter,
 *   <li>To configure the security for Version service.
 * </ul>
 *
 * @author ActiveViam
 */
@EnableGlobalAuthentication
@Configuration
public class CorsConfig implements ICorsConfig {

  /** The name of the Environment to use. */
  protected Environment env;

  /** The address the UI is exposed to. */
  public static final String ACTIVEUI_ADDRESS = "activeui.address";

  public CorsConfig(@Autowired Environment env) {
    this.env = env;
  }

  @Override
  public List<String> getAllowedOrigins() {
    return Collections.singletonList(env.getProperty(ACTIVEUI_ADDRESS, CorsConfiguration.ALL));
  }
}
