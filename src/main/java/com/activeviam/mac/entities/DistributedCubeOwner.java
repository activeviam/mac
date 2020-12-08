/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.entities;

/** Sub-class of owners for ActivePivot distributed cubes. */
public class DistributedCubeOwner extends CubeOwner {

	/**
	 * Constructor.
	 *
	 * @param id the unique id of the ActivePivot
	 */
	public DistributedCubeOwner(String id) {
		super(id);
	}

	@Override
	public OwnerType getType() {
		return OwnerType.DISTRIBUTED_CUBE;
	}
}
