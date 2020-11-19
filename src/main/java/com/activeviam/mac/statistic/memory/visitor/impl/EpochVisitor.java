/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

import com.activeviam.mac.entities.ChunkOwner;
import com.activeviam.mac.entities.CubeOwner;
import com.activeviam.mac.entities.NoOwner;
import com.activeviam.mac.entities.StoreOwner;
import com.google.common.collect.MultimapBuilder.SortedSetMultimapBuilder;
import com.google.common.collect.SortedSetMultimap;
import com.qfs.monitoring.statistic.IStatisticAttribute;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.PivotMemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.impl.ChunkSetStatistic;
import com.qfs.monitoring.statistic.memory.impl.ChunkStatistic;
import com.qfs.monitoring.statistic.memory.impl.DefaultMemoryStatistic;
import com.qfs.monitoring.statistic.memory.impl.DictionaryStatistic;
import com.qfs.monitoring.statistic.memory.impl.IndexStatistic;
import com.qfs.monitoring.statistic.memory.impl.ReferenceStatistic;
import com.qfs.monitoring.statistic.memory.visitor.IMemoryStatisticVisitor;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import java.util.OptionalLong;

public class EpochVisitor implements IMemoryStatisticVisitor<Void> {

	protected ChunkOwner owner = NoOwner.getInstance();
	protected TLongSet datastoreEpochs;
	protected SortedSetMultimap<ChunkOwner, Long> epochsPerOwner;

	public EpochVisitor() {
		this.datastoreEpochs = new TLongHashSet();
		// todo vlg: use another class for this? (unstable)
		this.epochsPerOwner = SortedSetMultimapBuilder
				.hashKeys()
				.treeSetValues()
				.build();
	}

	public TLongSet getDatastoreEpochs() {
		return datastoreEpochs;
	}

	public SortedSetMultimap<ChunkOwner, Long> getEpochsPerOwner() {
		return epochsPerOwner;
	}

	@Override
	public Void visit(
			DefaultMemoryStatistic memoryStatistic) {

		final OptionalLong epoch = readEpoch(memoryStatistic);

		switch (memoryStatistic.getName()) {
			case MemoryStatisticConstants.STAT_NAME_STORE:
				owner = new StoreOwner(
						memoryStatistic.getAttribute(
								MemoryStatisticConstants.ATTR_NAME_STORE_NAME)
								.asText());
				if (epoch.isPresent()) {
					datastoreEpochs.add(epoch.getAsLong());
				}
				visitChildren(memoryStatistic);
				break;
			case PivotMemoryStatisticConstants.STAT_NAME_PIVOT:
				owner = new CubeOwner(
						memoryStatistic.getAttribute(
								PivotMemoryStatisticConstants.ATTR_NAME_PIVOT_ID)
								.asText());
				break;
			default:
				visitChildren(memoryStatistic);
				break;
		}

		if (epoch.isPresent()) {
			epochsPerOwner.put(owner, epoch.getAsLong());
		}

		return null;
	}

	protected OptionalLong readEpoch(final IMemoryStatistic memoryStatistic) {
		final IStatisticAttribute epochAttr =
				memoryStatistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_EPOCH);
		if (epochAttr != null) {
			return OptionalLong.of(epochAttr.asLong());
		}
		return OptionalLong.empty();
	}

	protected void visitChildren(final IMemoryStatistic statistic) {
		if (statistic.getChildren() != null) {
			for (final IMemoryStatistic child : statistic.getChildren()) {
				child.accept(this);
			}
		}
	}

	@Override
	public Void visit(
			ChunkSetStatistic chunkSetStatistic) {
		return null;
	}

	@Override
	public Void visit(
			ChunkStatistic chunkStatistic) {
		return null;
	}

	@Override
	public Void visit(
			ReferenceStatistic referenceStatistic) {
		return null;
	}

	@Override
	public Void visit(
			IndexStatistic indexStatistic) {
		return null;
	}

	@Override
	public Void visit(
			DictionaryStatistic dictionaryStatistic) {
		return null;
	}
}
