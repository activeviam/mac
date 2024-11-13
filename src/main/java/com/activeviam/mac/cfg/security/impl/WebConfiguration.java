package com.activeviam.mac.cfg.security.impl;

public class WebConfiguration {
  final boolean useAnonymous = false;
  final String cookieName;
  final boolean logout;

  public WebConfiguration(String cookieName) {
    this.cookieName = cookieName;
    this.logout = cookieName != null;
  }
}
