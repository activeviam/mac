/*
 * (C) ActiveViam 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.qfs.pivot.monitoring.impl;

import com.activeviam.mac.Workaround;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.qfs.monitoring.statistic.IMonitoringStatistic;
import com.qfs.monitoring.statistic.IStatisticAttribute;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.visitor.IMemoryStatisticVisitor;
import com.quartetfs.fwk.QuartetRuntimeException;
import java.util.Collection;
import java.util.Map;

/**
 * Object ready for JSON serialization. It ensures in particular the order
 * of the serialized fields is correct and may be used to ignore (with
 * {@link JsonIgnore}) which field should be serialized or not and the
 * way fields are serialized.
 * This object cannot be visited by a {@link IMemoryStatisticVisitor visitor}.
 *
 * @author ActiveViam
 */
@JsonPropertyOrder({
		MemoryStatisticAdapter.NAME_ATTR,
		MemoryStatisticAdapter.ON_HEAP_ATTR,
		MemoryStatisticAdapter.OFF_HEAP_ATTR,
		MemoryStatisticAdapter.STATISTIC_CLASS_ATTR,
		MemoryStatisticAdapter.ATTRIBUTES_ATTR,
		MemoryStatisticAdapter.CHILDREN_ATTR
})
@Workaround(jira = "PIVOT-4093", solution = "Waiting for the next version with the fix")
public class MemoryStatisticAdapter implements IMemoryStatistic {

	/** Property name for the name of the statistics. */
	public static final String NAME_ATTR = "name";
	/** Property name for the quantity of heap memory used. */
	public static final String ON_HEAP_ATTR = "onheap";
	/** Property name for the quantity of off-heap memory used. */
	public static final String OFF_HEAP_ATTR = "offheap";
	/** Property name for the class of the serialized statistic. */
	public static final String STATISTIC_CLASS_ATTR = "statisticClass";
	/** Property name of the list of attributes of the statistic. */
	public static final String ATTRIBUTES_ATTR = "attributes";
	/** Property name of the list of children of the statistic. */
	public static final String CHILDREN_ATTR = "children";

	/** The underlying statistic. */
	protected IMemoryStatistic memoryStatistic;

	/**
	 * The implementation class of {@link IMemoryStatistic}.
	 */
	@JsonProperty(access = Access.READ_ONLY)
	// only for serialization
	protected String statisticClass;

	/**
	 * Constructor (use for serialization).
	 *
	 * @param memoryStatistic the wrapped {@link IMemoryStatistic}
	 */
	public MemoryStatisticAdapter(IMemoryStatistic memoryStatistic) {
		this(memoryStatistic, memoryStatistic.getClass().getName());
	}

	/**
	 * Constructor (To use for test only !).
	 *
	 * @param memoryStatistic the wrapped {@link IMemoryStatistic}
	 * @param statisticClass the class of the statistic.
	 */
	public MemoryStatisticAdapter(IMemoryStatistic memoryStatistic, String statisticClass) {
		this.memoryStatistic = memoryStatistic;
		this.statisticClass = statisticClass;
	}

	@Override
	public String getName() {
		return memoryStatistic.getName();
	}

	@JsonProperty("onheap")
	@JsonInclude(JsonInclude.Include.NON_DEFAULT) // do not serialize a value of 0L
	@Override
	public long getShallowOnHeap() {
		return memoryStatistic.getShallowOnHeap();
	}

	@Override
	public IMemoryStatistic setShallowOnHeap(long bytes) {
		return this;
	}

	@JsonProperty("offheap")
	@JsonInclude(JsonInclude.Include.NON_DEFAULT) // do not serialize a value of 0L
	@Override
	public long getShallowOffHeap() {
		return memoryStatistic.getShallowOffHeap();
	}

	@Override
	public IMemoryStatistic setShallowOffHeap(long bytes) {
		return this;
	}

	@JsonSerialize(using = MonitoringStatisticMapAttributesSerializer.class)
	@Override
	public Map<String, IStatisticAttribute> getAttributes() {
		Map<String, IStatisticAttribute> map = memoryStatistic.getAttributes();
		return map == null || map.isEmpty() ? null : map;
	}

	@Override
	public Collection<? extends IMemoryStatistic> getChildren() {
		Collection<? extends IMemoryStatistic> c = memoryStatistic.getChildren();
		return c == null || c.isEmpty() ? null : c;
	}

	@Override
	public void setParent(IMonitoringStatistic parent) {}

	// Ignored attributes

	@JsonIgnore
	@Override
	public long getRetainedOnHeap() {
		return memoryStatistic.getRetainedOnHeap();
	}

	@JsonIgnore
	@Override
	public long getRetainedOffHeap() {
		return memoryStatistic.getRetainedOffHeap();
	}

	@JsonIgnore
	@Override
	public IMemoryStatistic getParent() {
		return null;
	}

	// Useless methods for this implementation.

	@Override
	public MemoryStatisticAdapter setChildren(Collection<IMemoryStatistic> children) {
		return this;
	}

	@Override
	public MemoryStatisticAdapter setName(String name) {
		return this;
	}

	@Override
	public MemoryStatisticAdapter setAttributes(Map<String, IStatisticAttribute> attributes) {
		return this;
	}

	@Override
	public IMemoryStatistic append(IMemoryStatistic... children) {
		return null;
	}

	@Override
	public IMemoryStatistic append(Collection<? extends IMemoryStatistic> children) {
		return null;
	}

	@Override
	public <R> R accept(IMemoryStatisticVisitor<R> visitor) throws QuartetRuntimeException {
		throw new QuartetRuntimeException("This object is not visitable by any visitor!");
	}

	@Override
	public IMemoryStatistic getChildByName(String name) {
		return memoryStatistic.getChildByName(name);
	}
}
