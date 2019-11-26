/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac;

/**
 * Annotation marking code handling errors present in the Core Product.
 *
 * <p>When the support for a given version is dropped, all workarounds related to this version will
 * be removed.
 */
public @interface Workaround {

  /** @return Brief explanation of the issue to workaround. */
  String solution();

  /** @return Jira ticket related to the issue. */
  String jira() default "";
}
