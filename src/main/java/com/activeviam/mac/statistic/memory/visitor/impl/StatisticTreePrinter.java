/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.tech.observability.api.memory.IMemoryStatistic;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Statistics tree printer utility class.
 *
 * @author ActiveViam
 */
public class StatisticTreePrinter {

  /** One tree per root. */
  protected List<Tree> trees = new ArrayList<>();

  /**
   * Prints the descending tree of {@link IMemoryStatistic memory statistics} from the n {@link
   * Node} and downwards.
   *
   * @param sb {@link StringBuilder} on which the append the data
   * @param depth current depth
   * @param n current {@link Node}
   */
  protected static void print(StringBuilder sb, int depth, Node n) {
    depth++;
    for (Node child : n.children.values()) {
      for (int i = 0; i < depth; i++) {
        sb.append('\t');
      }
      sb.append(child.item.toString());
      sb.append(System.lineSeparator());
      print(sb, depth, child);
    }
    depth--;
  }

  /**
   * Returns the parent {@link IMemoryStatistic} of the input statistic.
   *
   * @param child statistic
   * @return tree List of the parents of the child statistic
   */
  public static List<IMemoryStatistic> getAscendingTree(IMemoryStatistic child) {
    IMemoryStatistic s = child;
    List<IMemoryStatistic> tree = new LinkedList<>();
    do {
      tree.add(s);
    } while ((s = s.getParent()) != null);

    Collections.reverse(tree);
    return tree;
  }

  /**
   * Returns the memory statistic Trees of the children of the input statistic.
   *
   * @param child memory statistic which children are to be displayed
   * @return the string displaying the tree
   */
  public static String getTreeAsString(IMemoryStatistic child) {
    StatisticTreePrinter p = new StatisticTreePrinter();
    p.add(child);
    return p.getTreesAsString();
  }

  /**
   * Checks is two {@link IMemoryStatistic} are equal.
   *
   * @param s1 first statistic
   * @param s2 second statistic
   * @return @code{true} if the statistics are equal @code{false} elsewhere.
   */
  protected static boolean areEquals(IMemoryStatistic s1, IMemoryStatistic s2) {
    return getId(s1) == getId(s2);
  }

  /**
   * Returns the debug ID of a statistic.
   *
   * @param statistic memory statistic
   * @return the {@link DatastoreConstants#CHUNK__DEBUG_TREE} id of the statistic
   */
  protected static long getId(IMemoryStatistic statistic) {
    return statistic.getAttributes().get(DebugVisitor.ID_KEY).asLong();
  }

  /**
   * Adds the statistic and all its childen to the tree.
   *
   * @param statistic statistic to add
   */
  public void add(IMemoryStatistic statistic) {
    List<IMemoryStatistic> ascendingTree = getAscendingTree(statistic);
    IMemoryStatistic root = ascendingTree.get(0);

    Tree t = null;
    for (Tree tree : this.trees) {
      if (areEquals(tree.root.item, root)) {
        t = tree;
        break;
      }
    }

    if (t == null) {
      // A tree with this root does not exist yet. We create it
      Node parent = new Node(ascendingTree.get(0));
      t = new Tree(parent);
      for (int i = 1; i < ascendingTree.size(); i++) {
        IMemoryStatistic next = ascendingTree.get(i);
        Node n = new Node(next);
        parent.addChild(n);
        parent = n;
      }
      this.trees.add(t);
    } else {
      Node parent = t.root;
      // Start at index 1 because we already know that the roots are equals
      for (int i = 1; i < ascendingTree.size(); i++) {
        IMemoryStatistic next = ascendingTree.get(i);
        Node c = parent.getChild(next);
        if (c == null) {
          c = new Node(next);
          parent.addChild(c);
        } else {
          // next is a child of parent. It means it belongs to the tree.
        }
        parent = c;
      }
    }
  }

  /**
   * returns the memory statistic Trees as a string.
   *
   * @return the string displaying the tree
   */
  public String getTreesAsString() {
    StringBuilder sb = new StringBuilder();
    for (Tree tree : this.trees) {
      Node root = tree.root;
      sb.append(root.item.toString());
      sb.append(System.lineSeparator());

      print(sb, 0, root);
    }
    return sb.toString();
  }

  /** Prints the tree of {@link IMemoryStatistic}. */
  public void print() {
    System.out.println(getTreesAsString());
  }

  ////////////////////////////////
  // Very basic tree structure //
  ////////////////////////////////

  private static class Tree {

    final Node root;

    private Tree(Node root) {
      this.root = root;
    }
  }

  private static class Node {

    final HashMap<Long, Node> children = new HashMap<>();
    final IMemoryStatistic item;

    private Node(final IMemoryStatistic item) {
      this.item = item;
    }

    private Node addChild(final Node node) {
      return this.children.put(getId(node.item), node);
    }

    private Node getChild(final IMemoryStatistic key) {
      return this.children.get(getId(key));
    }

    @Override
    public String toString() {
      return "Node{" + "childrenCount=" + this.children.size() + ", item=" + this.item + '}';
    }
  }
}
