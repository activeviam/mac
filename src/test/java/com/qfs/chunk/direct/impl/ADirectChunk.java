/*
 * (C) ActiveViam 2013
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.qfs.chunk.direct.impl;

import static com.quartetfs.fwk.util.MessageUtil.printDataSize;
import static com.quartetfs.fwk.util.MessageUtil.printTime;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.activeviam.mac.Workaround;
import com.qfs.chunk.IChunk;
import com.qfs.chunk.INativeMemoryAllocator;
import com.qfs.chunk.direct.allocator.impl.SlabMemoryAllocator;
import com.qfs.chunk.impl.AChunk;
import com.qfs.logging.MessagesDatastore;
import com.qfs.memory.impl.DirectMemory;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.impl.MemoryStatisticBuilder;
import com.qfs.util.impl.UnsafeUtil;

/**
 * Abstract base class for native memory chunks.
 * <p>
 * The default {@link #reset()} is empty, so do not forget to override it if needed.
 *
 * @author ActiveViam
 * @param <K> The type of the chunk.
 */
@Workaround(jira = "PIVOT-4041", solution = "Copy JDK11 implementation with fix")
@SuppressWarnings("restriction")
public abstract class ADirectChunk<K> extends AChunk<K> implements IChunk<K> {

	/** Logger */
	private static final Logger logger = MessagesDatastore.getLogger(ADirectChunk.class);

	/** Base address of the allocated memory */
	protected final long address;

	/** NUMA node on which the memory was allocated */
	protected final int numaNodeId;

	/** Size of the allocated memory in bytes */
	protected final int size;

	/** Memory cleaning action (thread behaviour) */
	protected final java.lang.ref.Cleaner.Cleanable cleanable;

	/** Memory cleaner (thread) */
	protected static final java.lang.ref.Cleaner cleaner = java.lang.ref.Cleaner.create();

	/**
	 * Allocate the chunk but do not initialize the memory.
	 *
	 * @param size size of the allocated memory in bytes
	 * @param allocator The memory allocator.
	 */
	public ADirectChunk(int size, INativeMemoryAllocator allocator) {
		this.numaNodeId = allocator.getNode();
		this.address = allocateMemory(allocator, size, numaNodeId, this);
		this.size = size;
		// Register a cleaner for that chunk
		this.cleanable = cleaner.register(this, new Deallocator(this.address, size, this.numaNodeId, allocator));
	}

	/**
	 * Constructor.
	 *
	 * @param size size of the allocated memory in bytes
	 * @param initialValue The initial value of all the bytes in this chunk
	 * @param allocator The memory allocator.
	 */
	public ADirectChunk(int size, byte initialValue, INativeMemoryAllocator allocator) {
		this(size, allocator);

		// Initialize the memory
		initialize(initialValue);
	}

	/**
	 * Allocates the given amount of memory using the given allocator, on the given NUMA node, and
	 * return a pointer to the allocated memory.
	 *
	 * @param allocator The allocator to use
	 * @param bytes The number of bytes to allocate
	 * @param numaNodeId The ID of the NUMA node on which to allocate memory
	 * @param inquirer The Object that request this allocation. Cannot be null.
	 * @return A pointer to the allocated memory.
	 */
	protected static long allocateMemory(
			final INativeMemoryAllocator allocator,
			final int bytes,
			final int numaNodeId,
			final Object inquirer) {
		final long address;
		try {
			DirectMemory.reserveMemory(bytes);
			try {
				long before = System.nanoTime();
				address = allocator.allocateMemory(bytes, numaNodeId);
				if (logger.isLoggable(Level.FINER)) {
					long elapsed = System.nanoTime() - before;
					logger.log(
							Level.FINER,
							inquirer.getClass().getSimpleName()
									+ " allocated "
									+ printDataSize(bytes)
									+ " of direct memory in "
									+ printTime(elapsed)
									+ ", at address="
									+ address);
				}
				return address;
			} catch (OutOfMemoryError x) {
				DirectMemory.unreserveMemory(bytes);
				throw x;
			}
		} catch (Throwable t) {
			// We catch all exceptions, because any of them if very bad and deserves to be logged.
			// It is important to do this before the printing of the slab allocator (just in case
			// the slab allocator printing throws as well).
			try {
				logger.log(
						Level.SEVERE,
						"Critical error: "
								+ inquirer.getClass().getSimpleName()
								+ " failed to allocate "
								+ printDataSize(bytes)
								+ " of direct memory."
								+ " Currently allocated amount of "
								+ printDataSize(DirectMemory.getReservedMemory()),
						t);

				// log the memory allocation for debugging.
				if (allocator instanceof SlabMemoryAllocator) {
					logger.log(Level.INFO, ((SlabMemoryAllocator) allocator).jmxPrintMemoryAllocation());
				}
			} catch (Throwable t2) {
				t.addSuppressed(t2);
			}
			throw t;
		}
	}

	/**
	 * Initializes the given amount of memory at the given address with the given value.
	 *
	 * @param address A pointer to the memory to initialize
	 * @param bytes The number of bytes to initialize
	 * @param value The value at which to initialize the memory area.
	 */
	protected static void initialize(long address, long bytes, byte value) {
		final boolean printTiming = logger.isLoggable(Level.FINER);
		final long start = printTiming ? System.nanoTime() : -1;

		UnsafeUtil.setMemory(address, bytes, value);

		if (printTiming) {
			logger.log(
					Level.FINER,
					"Initializing "
							+ printDataSize(bytes)
							+ " memory at address="
							+ address
							+ " took "
							+ printTime(System.nanoTime() - start));
		}
	}

	/**
	 * Initializes all the bytes of this direct chunk with the given value.
	 *
	 * @param value The value to initialize all bytes with.
	 */
	protected void initialize(byte value) {
		initialize(this.address, this.size, value);
	}

	/** Leaves the chunk untouched */
	@Override
	public void reset() {}

	@Override
	public Runnable destroy() {
		return destroyer(this);
	}

	static Runnable destroyer(ADirectChunk<?> adc) {
		final java.lang.ref.Cleaner.Cleanable cleanable = adc.cleanable;
		return cleanable::clean;
	}

	@Override
	protected long getOnHeapMemoryUsed() {
		return 16L + 8L + 8L;// FIXME take Cleaner size into account., one can use JOL to do that
	}

	@Override
	protected long getOffHeapMemoryUsed() {
		return size;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + " (address=" + address + ", size=" + printDataSize(size);
	}

	@Override
	public boolean isDead() {
		return false;
	}

	/**
	 * Used to free the memory of native chunks. Called by cleaners.
	 *
	 * @author ActiveViam
	 *
	 */
	static class Deallocator implements Runnable {

		/** Address of the memory section to free */
		volatile long address;

		/** Size of the chunk in bytes */
		final int size;

		/**  Id of the node on which the memory has been placed. */
		final int numaNodeId;

		/** The memory allocator. */
		final INativeMemoryAllocator allocator;

		public Deallocator(long address, int size, int numaNodeId, INativeMemoryAllocator allocator) {
			this.address = address;
			this.size = size;
			this.numaNodeId = numaNodeId;
			this.allocator = allocator;
		}

		@Override
		public void run() {
			try {
				if (address != 0) {
					if (logger.isLoggable(Level.FINER)) {
						logger.log(
								Level.FINER,
								"Deallocating " + printDataSize(size) + " memory at address=" + address);
					}
					allocator.freeMemory(address, size, numaNodeId);
					DirectMemory.unreserveMemory(size);
					address = 0;
				} else {
					logger.log(Level.WARNING, "Cleaning address twice. (" + printDataSize(size) + ").");
				}
			} catch (final Throwable t) {
				// Only cause of throwing would be a memory so full that it can't build this message or display logs.
				final String message = "Cannot properly release memory for node " + numaNodeId + " at @" + address;
				try {
					logger.log(Level.SEVERE, message, t);
				} catch (final Throwable tLog) {
					System.err.println(message);
					t.printStackTrace();
				}
			}
		}
	}

	@Override
	public IMemoryStatistic getMemoryStatistic() {
		return new MemoryStatisticBuilder()
				.withName(MemoryStatisticConstants.STAT_NAME_CHUNK)
				.withMemoryFootPrint(getOffHeapMemoryUsed(), getOnHeapMemoryUsed())
				.withCreatorClasses(getClass())
				.withAttribute(MemoryStatisticConstants.ATTR_NAME_CHUNK_ID, getChunkId())
				.withAttribute(MemoryStatisticConstants.ATTR_NAME_MEMORY_ADDRESS, this.address)
				.withAttribute(MemoryStatisticConstants.ATTR_NAME_NUMA_NODE, this.numaNodeId)
				.withAttribute(MemoryStatisticConstants.ATTR_NAME_LENGTH, capacity())
				.build();
	}

	/** Unsafe provider */
	protected static final sun.misc.Unsafe UNSAFE = UnsafeUtil.getUnsafe();

}

