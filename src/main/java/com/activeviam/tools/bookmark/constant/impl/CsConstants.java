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
public class CsConstants {

  public static class Paths {

    public static final String SEPARATOR = "/";

    public static final String UI = "ui";
    public static final String I18N_TOKEN = "i18n";
    public static final String SETTINGS_TOKEN = "settings";
    public static final String I18N = UI + SEPARATOR + Tree.BOOKMARKS + SEPARATOR + I18N_TOKEN;
    public static final String SETTINGS = UI + SEPARATOR + SETTINGS_TOKEN;

    public static final String BOOKMARK_LISTING = "bookmarks-listing";

    public static final String JSON = ".json";
    public static final String I18N_JSON = SEPARATOR + I18N_TOKEN + JSON;
    public static final String SETTINGS_JSON = SEPARATOR + SETTINGS_TOKEN + JSON;

    public static final String METADATA = "_metadata";
    public static final String METADATA_FILE = METADATA + JSON;
    public static final String FOLDER_TEMPLATE_FILE = "folder_template.json";
    public static final String BOOKMARK_TEMPLATE_FILE = "bookmark_template.json";
  }

  public static class Tree {

    public static final String CONTENT = "content";
    public static final String STRUCTURE = "structure";
    public static final String BOOKMARKS = "bookmarks";
    public static final String ENTRY = "entry";
    public static final String NAME = "name";
    public static final String VALUE = "value";
    public static final String TYPE = "type";
    public static final String DESCRIPTION = "description";
    public static final String KEY = "key";
  }

  public static class Role {

    public static final String OWNERS = "owners";
    public static final String READERS = "readers";
    public static final String ROLE_CS_ROOT = "ROLE_CS_ROOT";
    public static final String ROLE_USER = "ROLE_USER";
  }

  public static class Content {

    public static final String FILTER = "filter";
    public static final String CONTAINER = "container";
    public static final String MDX = "mdx";
    public static final String FOLDER = "folder";
  }

  /**
   * Gets the type of a bookmark from its name.
   *
   * @param contentType name of the type
   * @return exported type
   */
  public static String getBookmarkType(String contentType) {
    switch (contentType) {
      case Content.FILTER:
        return Content.MDX;
      default:
        return Content.CONTAINER;
    }
  }
}

// CHECKSTYLE.ON: JavadocType
// CHECKSTYLE.ON: JavadocVariable
