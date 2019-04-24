/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.statistic.memory.visitor.impl;

import com.qfs.monitoring.statistic.memory.IMemoryStatistic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class StatisticTreePrinter {

	/**
	 * One tree per root
	 */
	protected List<Tree> trees = new ArrayList<>();

	public void add(IMemoryStatistic statistic) {
		List<IMemoryStatistic> ascendingTree = getAscendingTree(statistic);
		IMemoryStatistic root = ascendingTree.get(0);

		Tree t = null;
		for (Tree tree : trees) {
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

	public String getTreesAsString() {
		StringBuilder sb = new StringBuilder();
		for (Tree tree : trees) {
			Node root = tree.root;
			sb.append(root.item.toString());
			sb.append(System.lineSeparator());

			print(sb, 0, root);
		}
		return sb.toString();
	}

	public void print() {
		System.out.println(getTreesAsString());
	}

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

	public static List<IMemoryStatistic> getAscendingTree(IMemoryStatistic child) {
		IMemoryStatistic s = child;
		List<IMemoryStatistic> tree = new LinkedList<>();
		do {
			tree.add(s);
		} while ((s = s.getParent()) != null);

		Collections.reverse(tree);
		return tree;
	}

	public static String getTreeAsString(IMemoryStatistic child) {
		StatisticTreePrinter p = new StatisticTreePrinter();
		p.add(child);
		return p.getTreesAsString();
	}

	protected static boolean areEquals(IMemoryStatistic s1, IMemoryStatistic s2) {
		return getId(s1) == getId(s2);
	}

	protected static long getId(IMemoryStatistic statistic) {
		return statistic.getAttributes().get(DebugVisitor.ID_KEY).asLong();
	}

	////////////////////////////////
	// Very basic tree structure  //
	////////////////////////////////

	protected static class Tree {

		final Node root;

		protected Tree(Node root) {
			this.root = root;
		}

	}

	protected static class Node {

		final HashMap<Long, Node> children = new HashMap<>();
		final IMemoryStatistic item;

		protected Node(final IMemoryStatistic item) {
			this.item = item;
		}

		protected Node addChild(final Node node) {
			return children.put(getId(node.item), node);
		}

		protected Node getChild(final IMemoryStatistic key) {
			return children.get(getId(key));
		}

		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder("Node{");
			sb.append("childrenCount=").append(children.size());
			sb.append(", item=").append(item);
			sb.append('}');
			return sb.toString();
		}
	}
}
