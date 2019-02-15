/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.statistic.memory.visitor.impl;

public enum ProviderCpnType {

	POINT_INDEX("PointIndex"),
	POINT_MAPPING("PointMapping"),
	AGGREGATE_STORE("AggregateStore"),
	BITMAP_MATCHER("BitmapMatcher");

	final String friendlyName;

	ProviderCpnType(String friendlyName) {
		this.friendlyName = friendlyName;
	}

	@Override
	public String toString() {
		return this.friendlyName;
	}
}
