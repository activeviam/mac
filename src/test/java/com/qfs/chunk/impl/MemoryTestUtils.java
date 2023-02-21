package com.qfs.chunk.impl;

import com.activeviam.mac.Workaround;
import com.qfs.pool.impl.NamedThread;
import com.qfs.util.impl.QfsReflection;
import com.qfs.vector.IVectorAllocator;
import com.qfs.vector.impl.Vectors;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

public class MemoryTestUtils {

  /**
   * Resets all the vector allocators of all threads.
   *
   * <p>Note that this function is racy, so should only be used in tests.
   */
  @Workaround(
      solution = "Force 5+ releases to fill the VectorAllocator ResourcePool queue",
      jira = "PIVOT-6621")
  public static void resetAllThreadsVectorAllocator() {
    final int nbrActives = Thread.activeCount();
    final Thread[] allThreads = new Thread[nbrActives * 2];
    final Field directAllocatorsField =
        QfsReflection.findField(NamedThread.class, "directVectorAllocators");
    final Field transientAllocatorsField =
        QfsReflection.findField(NamedThread.class, "transientVectorAllocators");
    Thread.enumerate(allThreads);
    for (final Thread t : allThreads) {
      if (t instanceof NamedThread) {
        final IVectorAllocator[] directAllocators =
            QfsReflection.getFieldValue(directAllocatorsField, t);
        final IVectorAllocator[] transientAllocators =
            QfsReflection.getFieldValue(transientAllocatorsField, t);
        Stream.concat(Arrays.stream(directAllocators), Arrays.stream(transientAllocators))
            .filter(Objects::nonNull)
            .forEach(
                v -> {
                  for (int i = 0; i < Vectors.VECTOR_ALLOCATOR_POOL_SIZE + 1; i++) {
                    v.release();
                    // Release each existing allocator VECTOR_ALLOCATOR_POOL_SIZE + 1 times to make
                    // sure
                    // it's evicted from the resource pool
                  }
                });
      }
    }
  }
}
