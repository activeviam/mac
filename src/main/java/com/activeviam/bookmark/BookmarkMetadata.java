/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.bookmark;

import java.util.List;
import java.util.Map;

/** @author ActiveViam */
public class BookmarkMetadata {
  private String description;
  private Map<String, List<String>> permissions;
  private String key;

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getDescription() {
    return description;
  }

  public Map<String, List<String>> getPermissions() {
    return permissions;
  }

  public void setPermissions(Map<String, List<String>> permissions) {
    this.permissions = permissions;
  }
}
