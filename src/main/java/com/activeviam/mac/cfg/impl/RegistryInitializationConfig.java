/*
 * (C) ActiveViam 2024
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.cfg.impl;

import com.activeviam.tech.core.api.registry.Registry;
import com.activeviam.tech.core.api.registry.Registry.RegistryContributions;
import java.util.List;
import org.springframework.boot.context.event.ApplicationStartingEvent;
import org.springframework.context.ApplicationListener;

/**
 * Component initializing the registry as soon as possible when an application is starting.
 *
 * @author ActiveViam
 */
public class RegistryInitializationConfig implements ApplicationListener<ApplicationStartingEvent> {

  public void onApplicationEvent(final ApplicationStartingEvent ignored) {
    setupRegistry();
  }

  public static void setupRegistry() {
    Registry.initialize(
        RegistryContributions.builder().packagesToScan(List.of("com.activeviam.mac")).build());
  }
}
