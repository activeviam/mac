/*
 * (C) Quartet FS 2017
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.qfs.monitoring.cfg.security.impl;

import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import com.qfs.monitoring.cfg.security.impl.ASecurityConfig.AVersionSecurityConfigurer;


/**
 * To expose the Version REST service.
 *
 * @author ActiveViam
 */
@Configuration
@Order(3)
public class VersionSecurityConfigurer extends AVersionSecurityConfigurer {}
