/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.tools.bookmark.constant.impl;

// CHECKSTYLE.OFF: JavadocVariable (Internal constants)
// CHECKSTYLE.OFF: JavadocType (Internal constants)

/**
 * Constants to use to export bookmarks from the Content Server.
 *
 * @author ActiveViam
 */
public class ContentServerConstants {

  public static class Paths {

    public static final String SEPARATOR = "/";

    public static final String UI = "ui";
    public static final String DASHBOARDS = UI + SEPARATOR + Tree.DASHBOARDS;
    public static final String WIDGETS = UI + SEPARATOR + Tree.WIDGETS;
    public static final String JSON = ".json";
    public static final String INITIAL_CONTENT = "initial_content.json";
  }

  public static class Tree {

    public static final String DASHBOARDS = "dashboards";
    public static final String WIDGETS = "widgets";
  }

  public static class Role {

    public static final String OWNERS = "owners";
    public static final String READERS = "readers";
    public static final String ROLE_CS_ROOT = "ROLE_CS_ROOT";
    public static final String ROLE_USER = "ROLE_USER";
  }
}

// CHECKSTYLE.ON: JavadocType
// CHECKSTYLE.ON: JavadocVariable
