/*
 * (C) Quartet FS 2018
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.qfs.monitoring.cfg.impl;

import com.qfs.server.cfg.IActivePivotManagerDescriptionConfig;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.impl.DescriptionUtil;
import com.quartetfs.fwk.serialization.impl.JaxbSerializer;

/**
 * @author Quartet FS
 *
 */
public class MonitoringManagerDescriptionConfig implements IActivePivotManagerDescriptionConfig {

	@Override
	public IActivePivotManagerDescription managerDescription() {
		return DescriptionUtil.loadDescription("DESC-INF/ActivePivotManager.xml", new JaxbSerializer());
	}

}
