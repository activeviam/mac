/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.statistic.memory.visitor.impl;

import com.qfs.monitoring.statistic.IStatisticAttribute;
import com.qfs.monitoring.statistic.impl.IntegerStatisticAttribute;
import com.qfs.monitoring.statistic.impl.LongStatisticAttribute;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.impl.ChunkSetStatistic;
import com.qfs.monitoring.statistic.memory.impl.ChunkStatistic;
import com.qfs.monitoring.statistic.memory.impl.DefaultMemoryStatistic;
import com.qfs.monitoring.statistic.memory.impl.DictionaryStatistic;
import com.qfs.monitoring.statistic.memory.impl.IndexStatistic;
import com.qfs.monitoring.statistic.memory.impl.ReferenceStatistic;
import com.qfs.monitoring.statistic.memory.visitor.IMemoryStatisticVisitor;

import java.util.concurrent.atomic.AtomicLong;

public class DebugVisitor implements IMemoryStatisticVisitor<Void> {

	protected static final String DEPTH_KEY = "debug-depth";
	protected static final String ID_KEY = "debug-id";

	protected final AtomicLong id = new AtomicLong(0);

	protected int depth = -1;

	public static StatisticTreePrinter debug(IMemoryStatistic root) {
		root.accept(new DebugVisitor());
		return new StatisticTreePrinter();
	}

//	public static void printAscendingTree(IMemoryStatistic child) {
//		IMemoryStatistic s = child;
//		List<IMemoryStatistic> tree = new LinkedList<>();
//		do {
//			tree.add(s);
//		} while ((s = s.getParent()) != null);
//
//		Iterator<IMemoryStatistic> it = ((LinkedList<IMemoryStatistic>) tree).descendingIterator();
//		int depth = 0;
//		StringBuilder sb = new StringBuilder();
//		while (it.hasNext()) {
//			IMemoryStatistic next = it.next();
//			for (int i = 0; i < depth; i++) {
//				sb.append('\t');
//			}
//			sb.append(next.toString());
//			if (it.hasNext()) {
//				sb.append(System.lineSeparator());
//			}
//			depth++;
//		}
//
//		System.out.println(sb.toString());
//	}

	protected void setParent(IMemoryStatistic parent) {
		addDebugAttributes(parent);
		if (parent.getChildren() == null) {
			return;
		}

		depth++;
		for (IMemoryStatistic child : parent.getChildren()) {
			child.setParent(parent);
			addDebugAttributes(child);
			child.accept(this);
		}
		depth--;
	}

	protected void addDebugAttributes(IMemoryStatistic statistic) {
		IntegerStatisticAttribute depthValue = new IntegerStatisticAttribute(this.depth);
		IStatisticAttribute old = statistic.getAttributes().put(DEPTH_KEY, depthValue);
		if (old != null && !old.equals(depthValue)) {
			throw new RuntimeException(DEPTH_KEY + " an depthValue already exists and are not equal: old=" + old + "; new=" + depthValue);
		}
		statistic.getAttributes().computeIfAbsent(ID_KEY, k -> new LongStatisticAttribute(this.id.getAndIncrement()));
	}

	@Override
	public Void visit(DefaultMemoryStatistic memoryStatistic) {
		setParent(memoryStatistic);
		return null;
	}

	@Override
	public Void visit(ChunkSetStatistic chunkSetStatistic) {
		setParent(chunkSetStatistic);
		return null;
	}

	@Override
	public Void visit(ChunkStatistic chunkStatistic) {
		setParent(chunkStatistic);
		return null;
	}

	@Override
	public Void visit(ReferenceStatistic referenceStatistic) {
		setParent(referenceStatistic);
		return null;
	}

	@Override
	public Void visit(IndexStatistic indexStatistic) {
		setParent(indexStatistic);
		return null;
	}

	@Override
	public Void visit(DictionaryStatistic dictionaryStatistic) {
		setParent(dictionaryStatistic);
		return null;
	}
}
