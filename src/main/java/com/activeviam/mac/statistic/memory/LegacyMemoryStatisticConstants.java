/*
 * (C) ActiveViam 2026
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.activeviam.tech.observability.internal.memory.MemoryStatisticConstants;

/**
 * Names of memory statistics that are no longer defined in {@link MemoryStatisticConstants}, but
 * that can still be found in statistics exported by older versions of Atoti Server.
 *
 * @author ActiveViam
 */
public final class LegacyMemoryStatisticConstants {

  /** Name of the statistic of a chunk entry (e.g. of a vector). */
  public static final String STAT_NAME_CHUNK_ENTRY = "ChunkEntry";

  /** Name of the statistic grouping the primary indices of a store partition. */
  public static final String STAT_NAME_PRIMARY_INDICES = "PrimaryIndices";

  private LegacyMemoryStatisticConstants() {}
}
