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
import lombok.Data;

/**
 * Internal representation of the SnapshotContentTree object, aimed to simplify the object to the
 * components required to generate the bookmark hierarchy.
 *
 * @author ActiveViam
 */
@Data
public class SnapshotNode {

  /** Key of this node. */
  private String key;
  /** Path to this node from the root. */
  private String path = "";
  /** Content of this node. */
  private BasicJsonContentEntry entry;
  /** Child nodes for this node. */
  private List<SnapshotNode> children;

  /**
   * Constructor.
   *
   * @param key key of the node
   */
  public SnapshotNode(String key) {
    this.key = key;
    children = new ArrayList<>();
  }

  /**
   * Adds a node as a child of this node.
   *
   * @param c node to add
   */
  public void addChild(SnapshotNode c) {
    children.add(c);
  }
}
