/*
 * (C) ActiveViam 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.cfg.impl;

import com.activeviam.web.spring.internal.config.ASpringResourceServerConfig;
import java.util.Collections;
import java.util.Set;
import org.springframework.context.annotation.Configuration;

/**
 * Spring configuration for Atoti UI web application.
 *
 * @author ActiveViam
 */
@Configuration
public class ActiveUiResourceServerConfig extends ASpringResourceServerConfig {

  /** The namespace of the Atoti UI web application. */
  public static final String NAMESPACE = "ui";

  /** Constructor. */
  public ActiveUiResourceServerConfig() {
    super("/" + NAMESPACE);
  }

  @Override
  protected void registerRedirections(final ResourceRegistry registry) {
    super.registerRedirections(registry);
    // Redirect from the root to ActiveUI
    registry.redirectTo(NAMESPACE + "/index.html", "/");
    // Redirect the calls to env*.js to the AP ones rather than the default of the ActiveUI apps
    registry.serve("/content/ui/env*.js").addResourceLocations("classpath:/static/content/");
    registry.serve("/ui/env*.js").addResourceLocations("classpath:/static/activeui/");
    registerExtensions(registry);
  }

  protected void registerExtensions(final ResourceRegistry registry) {
    registry.serve("/ui/extensions*.json").addResourceLocations("classpath:/static/activeui/");
    registry
        .serve("/ui/extensions/text-editor-extension/**/*.js")
        .addResourceLocations("classpath:/static/activeui/extensions/text-editor-extension/");
  }

  /**
   * Registers resources to serve.
   *
   * @param registry registry to use
   */
  @Override
  protected void registerResources(final ResourceRegistry registry) {
    super.registerResources(registry);

    // ActiveUI web app also serves request to the root, so that the redirection from root to
    // ActiveUI works
    registry
        .serve("/")
        .addResourceLocations("/", "classpath:META-INF/resources/")
        .setCacheControl(getDefaultCacheControl());
  }

  /**
   * Gets the extensions of files to serve.
   *
   * @return all files extensions
   */
  @Override
  public Set<String> getServedExtensions() {
    return Set.of(
        // Default HTML files
        "html",
        "js",
        "css",
        "map",
        "json",
        // Image extensions
        "png",
        "jpg",
        "gif",
        "ico",
        // Font extensions
        "eot",
        "svg",
        "ttf",
        "woff",
        "woff2");
  }

  @Override
  public Set<String> getServedDirectories() {
    return Collections.singleton("/");
  }

  @Override
  public Set<String> getResourceLocations() {
    // ActiveUI is integrated in the sandbox project thanks to Maven integration.
    // You can read more about this feature here
    // https://support.activeviam.com/documentation/activeui/4.2.0/dev/setup/maven-integration.html

    return Set.of(
        "/activeui/", // index.html, favicon.ico, etc.
        "classpath:META-INF/resources/webjars/activeui/"); // ActiveUI SDK UMD scripts
    // and supporting assets
  }
}
