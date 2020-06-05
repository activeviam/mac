/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.tools.bookmark.node.impl;

import com.qfs.content.snapshot.impl.BasicJsonContentEntry;
import java.util.ArrayList;
import java.util.List;

/**
 * Internal representation of the SnapshotContentTree object, aimed to simplify the object to the
 * components required to generate the bookmark hierarchy.
 *
 * @author ActiveViam
 */
public class SnapshotNode {
  private String key;
  private String path = "";
  private BasicJsonContentEntry entry;
  private List<SnapshotNode> children;

  public SnapshotNode(String key) {
    this.key = key;
    children = new ArrayList<>();
  }

  public BasicJsonContentEntry getEntry() {
    return entry;
  }

  public void setEntry(BasicJsonContentEntry entry) {
    this.entry = entry;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public void addChild(SnapshotNode c) {
    children.add(c);
  }

  public List<SnapshotNode> getChildren() {
    return children;
  }

  public void setChildren(List<SnapshotNode> children) {
    this.children = children;
  }
}
