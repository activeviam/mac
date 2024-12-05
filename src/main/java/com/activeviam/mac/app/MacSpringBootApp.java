/*
 * (C) ActiveViam 2013-2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.app;

import com.activeviam.mac.cfg.impl.MacServerConfig;
import jakarta.servlet.MultipartConfigElement;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.DispatcherServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

/**
 * SpringBoot starter class for ActivePivot.
 *
 * <p>We don't use {@link SpringBootApplication} here because it will load too many beans including
 * those we don't need and causing bean conflict
 *
 * @author ActiveViam
 */
@Configuration
@EnableAutoConfiguration
@EnableWebMvc
@Import({MacServerConfig.class})
public class MacSpringBootApp {

  /**
   * Starts the Memory Analysis Cube application.
   *
   * @param args additional CLI arguments
   */
  public static void main(final String[] args) {
    SpringApplication.run(MacSpringBootApp.class, args);
  }

  /**
   * Provides a customized {@link DispatcherServletRegistrationBean} with a specific servlet path.
   *
   * <p>This bean is required to make AP work in SpringBoot; see :
   * https://github.com/spring-projects/spring-boot/issues/15373
   *
   * @param dispatcherServlet the dispatcher servlet
   * @param multipartConfig the dispatcher servlet properties
   * @return a customized {@link DispatcherServletRegistrationBean}
   */
  @Bean
  public DispatcherServletRegistrationBean dispatcherServletRegistration(
      final DispatcherServlet dispatcherServlet,
      final ObjectProvider<MultipartConfigElement> multipartConfig) {
    final DispatcherServletRegistrationBean registration =
        new DispatcherServletRegistrationBean(dispatcherServlet, "/*");
    registration.setName("springDispatcherServlet");
    registration.setLoadOnStartup(1);
    multipartConfig.ifAvailable(registration::setMultipartConfig);
    return registration;
  }
}
