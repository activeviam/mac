/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.junit;

import com.activeviam.copper.CopperRegistrations;
import com.qfs.monitoring.memory.impl.OnHeapPivotMemoryQuantifierPlugin;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.contributions.impl.ClasspathContributionProvider;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class RegistrySetupExtension implements BeforeAllCallback {

	@Override
	public void beforeAll(ExtensionContext context) {
		CopperRegistrations.setupRegistryForTests(OnHeapPivotMemoryQuantifierPlugin.class);
		Registry.setContributionProvider(new ClasspathContributionProvider());
	}
}
