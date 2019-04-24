/*
 * (C) ActiveViam 2017
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.cfg.security.impl;

import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;


/**
 * To expose the Version REST service.
 *
 * @author ActiveViam
 */
@Configuration
@Order(3)
public class VersionSecurityConfigurer extends ASecurityConfig.AVersionSecurityConfigurer {}
