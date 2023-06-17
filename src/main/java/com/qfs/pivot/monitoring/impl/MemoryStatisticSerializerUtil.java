/*
 * (C) ActiveViam 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.qfs.pivot.monitoring.impl;

import static com.activeviam.mac.cfg.impl.SourceConfig.resolveDirectory;

import com.activeviam.fwk.ActiveViamRuntimeException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.qfs.concurrent.cancellable.impl.CancellableCountedCompleter;
import com.qfs.concurrent.cancellable.impl.MultiCancellableCountedCompleter;
import com.qfs.jackson.impl.JacksonSerializer;
import com.qfs.monitoring.statistic.IMonitoringStatistic;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.pool.IThreadPoolSelector;
import com.qfs.util.impl.QfsConcurrency;
import com.quartetfs.fwk.impl.Pair;
import com.quartetfs.fwk.serialization.SerializerException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountedCompleter;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Data;
import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;

/**
 * Utility class to serialize/deserialize {@link IMemoryStatistic}s.
 *
 * @author ActiveViam
 */
public class MemoryStatisticSerializerUtil {

  /** Extension of the files that contain the dumped memory statistics. */
  public static final String JSON_FILE_EXTENSION = "json";

  /** Extension of the compressed files. */
  public static final String COMPRESSED_FILE_EXTENSION = "sz";

  private MemoryStatisticSerializerUtil() {}

  public static void main(String[] args) throws IOException {
    final var directory = resolveDirectory("input_statistics\\folder_1675616797942");

    // warmup
    for (int i = 0; i < 20; i++) {
      var filePath = directory.resolve("store_City.json").toAbsolutePath();
      var filePathComp = directory.resolve("store_City.json.sz").toAbsolutePath();
      MemoryStatisticSerializerUtil.readStatisticFile(filePath.toFile());
      MemoryStatisticSerializerUtil.readStatisticFile(filePathComp.toFile());
    }
    Runtime.getRuntime().gc();

    System.out.println("Warmup done");
    // Do on "real" files now
    final var filePath = directory.resolve("pivot_StandardisedApproachCube.json").toAbsolutePath();
    final var filePathComp =
        directory.resolve("pivot_StandardisedApproachCube.json.sz").toAbsolutePath();

    long start = System.nanoTime();
    final IMemoryStatistic val = MemoryStatisticSerializerUtil.readStatisticFile(filePath.toFile());
    System.out.println("Deserialize  : " + (System.nanoTime() - start) / 1_000_000_000d + "s");

    start = System.nanoTime();
    final IMemoryStatistic valComp =
        MemoryStatisticSerializerUtil.readStatisticFile(filePathComp.toFile());
    System.out.println("Deserialize Snappied : " + (System.nanoTime() - start) / 1_000_000_000d + "s");

    assert valComp.equals(val);
  }

  static {
    final SimpleModule deserializeModule = new SimpleModule();
    deserializeModule.addDeserializer(
        IMonitoringStatistic.class, new MonitoringStatisticDeserializer());
    deserializeModule.addDeserializer(IMemoryStatistic.class, new MemoryStatisticDeserializer());
    deserializeModule.addDeserializer(
        MemoryStatisticDeserializerHelper.class,
        new MemoryStatisticDeserializerHelperDeserializer());

    JacksonSerializer.getObjectMapper().registerModule(deserializeModule);
  }

  /**
   * Serializes a statistic into a given writer.
   *
   * @param activeStatistic the {@link IMonitoringStatistic} to serialize.
   * @param writer the writer in which the data will be serialized.
   */
  public static void serialize(IMonitoringStatistic activeStatistic, Writer writer) {
    try {
      JacksonSerializer.serialize(
          new MonitoringStatisticAdapter(activeStatistic), new BufferedWriter(writer));
    } catch (SerializerException e) {
      throw new ActiveViamRuntimeException(e);
    }
  }

  /**
   * Serializes a statistic into a given writer.
   *
   * @param statistic the {@link IMemoryStatistic} to serialize.
   * @param writer the writer in which the data will be serialized.
   */
  public static void serialize(IMemoryStatistic statistic, Writer writer) {
    try {
      JacksonSerializer.serialize(
          statistic.accept(new SerializerVisitor()), new BufferedWriter(writer));
    } catch (SerializerException e) {
      throw new ActiveViamRuntimeException(e);
    }
  }

  /**
   * Deserializes a statistic from a reader into a given type.
   *
   * @param <T> The type of the statistic.
   * @param streamReader the {@link IMonitoringStatistic} to deserialize.
   * @param klass the type of {@link IMonitoringStatistic}.
   * @return the deserialized statistic
   */
  public static <T extends IMonitoringStatistic> T deserialize(
      final Reader streamReader, final Class<T> klass) {
    final ObjectReader reader = JacksonSerializer.getObjectMapper().readerFor(klass);
    try {
      return reader.readValue(new BufferedReader(streamReader));
    } catch (IOException e) {
      throw new ActiveViamRuntimeException(e);
    }
  }

  @Data
  public static class MemoryStatisticDeserializerHelper {

    //public static final long PARALLEL_THRESHOLD = 10_000_000L;
    public static final long PARALLEL_THRESHOLD = Long.MAX_VALUE;
    public static final long SUBTASK_MIN_SIZE = 10_000_000L;

    protected final Map<Integer, List<Pair<Long, Long>>> rangesPerDepth;
    protected final long fileLength;
  }

  private static <T extends IMonitoringStatistic> T doDeserializeAsync(
      final File file,
      final Class<T> klass,
      final MemoryStatisticDeserializerHelper helper,
      final ForkJoinPool forkJoinPool)
      throws IOException {

    final boolean isCompressedFile = file.getName().endsWith("." + COMPRESSED_FILE_EXTENSION);
    // Only run parallel treatment if the file length is below
    // MemoryStatisticDeserializerHelper.PARALLEL_THRESHOLD
    if (helper.fileLength >= MemoryStatisticDeserializerHelper.PARALLEL_THRESHOLD) {
      try (var readersHandler = generateThreadLocalReaders(file, helper)) {
        // Try to generate deserialization tasks with a length as close to taskSize that will cover
        // the entire file
        DeserializerTask<T> headTask =
            generateTasks(helper, forkJoinPool, klass, MemoryStatisticDeserializerHelper.SUBTASK_MIN_SIZE, readersHandler);
        var tasks = new ArrayList<>(headTask.getChildren());
        tasks.add(headTask);
        QfsConcurrency.smartForkAll(tasks.toArray(CancellableCountedCompleter[]::new));
        // Is that too low ? FRTB was taking 20+ minutes without load
        return headTask.get(100, TimeUnit.MINUTES);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

    } else {
      // Single-Threaded parsing
      InputStream inputStream = new FileInputStream(file);
      if (isCompressedFile) {
        inputStream = new SnappyFramedInputStream(inputStream);
      }

      try (final InputStreamReader reader =
          new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
        return deserialize(reader, klass);
      }
    }
  }

  // Local thread -> reader map to have control over the readers' lifecycle
  // (ThreadLocals only die when the thread dies)
  public static class ReadersHandler implements AutoCloseable {

    private final File fileToRead;

    private final MemoryStatisticDeserializerHelper helper;
    protected final Map<Thread, MarkableFileInputStreamReader> localReaders;

    public ReadersHandler(File fileToRead, MemoryStatisticDeserializerHelper helper) {
      this.fileToRead = fileToRead;
      this.helper = helper;
      this.localReaders = new ConcurrentHashMap<>();
    }

    @Override
    public void close() {
      localReaders.forEach(
          (thread, reader) -> {
            try {
              reader.close();
            } catch (IOException e) {
              throw new ActiveViamRuntimeException(e);
            }
          });
    }

    public MarkableFileInputStreamReader getReader() throws IOException {
      var curReader = localReaders.get(Thread.currentThread());
      if (curReader == null) {
        final boolean isCompressedFile =
            fileToRead.getName().endsWith("." + COMPRESSED_FILE_EXTENSION);
        MarkableFileInputStream inputStream =
            new MarkableFileInputStream(new FileInputStream(fileToRead));
        if (isCompressedFile) {
          throw new IllegalStateException(
              "Multi-threaded deserialization is no supported on snappy files."
                  + " Please un-snappy the sources with the provided command to process parallel deserialization");
        }
        curReader = new MarkableFileInputStreamReader(inputStream, StandardCharsets.UTF_8);
        curReader.mark((int) helper.fileLength);
        localReaders.put(Thread.currentThread(), curReader);
      } else {
        // FIXME : allow reset to non-zero pos
        curReader.reset();
      }
      return curReader;
    }
  }

  public static class MarkableFileInputStreamReader extends InputStreamReader {

    private final MarkableFileInputStream inputStream;

    public MarkableFileInputStreamReader(MarkableFileInputStream in, Charset charset) {
      super(in, charset);
      this.inputStream = in;
    }

    @Override
    public boolean markSupported() {
      return true;
    }

    @Override
    public void mark(int readAheadLimit) {
      inputStream.mark(readAheadLimit);
    }

    @Override
    public void reset() throws IOException {
      inputStream.myFileChannel.position(inputStream.mark);
    }

    protected Reader truncate(long startPos, long endPos) {
      final int length = (int) (endPos - startPos);
      byte[] buffer = new byte[length];
      try {
        inputStream.myFileChannel.position(startPos);
        inputStream.read(buffer, 0, length);
        return new InputStreamReader(new ByteArrayInputStream(buffer), StandardCharsets.UTF_8);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class MarkableFileInputStream extends FilterInputStream {

    private FileChannel myFileChannel;
    private long mark = -1;

    public MarkableFileInputStream(FileInputStream fis) {
      super(fis);
      myFileChannel = fis.getChannel();
    }

    @Override
    public boolean markSupported() {
      return true;
    }

    @Override
    public synchronized void mark(int readlimit) {
      try {
        mark = myFileChannel.position();
      } catch (IOException ex) {
        mark = -1;
      }
    }

    @Override
    public synchronized void reset() throws IOException {
      if (mark == -1) {
        throw new IOException("not marked");
      }
      myFileChannel.position(mark);
    }
  }

  private static ReadersHandler generateThreadLocalReaders(
      File file, MemoryStatisticDeserializerHelper helper) {
    return new ReadersHandler(file, helper);
  }

  public static class DeserializerTask<T extends IMonitoringStatistic>
      extends MultiCancellableCountedCompleter<T> {

    public static final String COMPUTED_SUBTASKS_ATTRIBUTE = "SubTaskData";

    public static final String CURRENT_TASK_POSITION_ATTRIBUTE = "ParentPos";
    final Class<T> klass;
    final ReadersHandler reader;
    final int depth;
    final long start;
    final long end;

    T result;

    @Override
    public String toString() {
      return "DeserializerTask{" + "depth=" + depth + ", start=" + start + ", end=" + end + '}';
    }

    protected DeserializerTask(
        CancellableCountedCompleter<T> completer,
        IThreadPoolSelector poolSelector,
        Class<T> klass,
        ReadersHandler reader,
        int depth,
        long start,
        long end) {

      super(completer, poolSelector);
      this.klass = klass;
      this.reader = reader;
      this.depth = depth;
      this.start = start;
      this.end = end;
    }

    @Override
    protected void computeSafely() {
      // Nothing
    }

    @Override
    protected void onRegularCompletion(CountedCompleter<?> caller) throws Throwable {
      super.onRegularCompletion(caller);
      this.setRawResult(runDeserialization());
      this.quietlyComplete();
    }

    @Override
    protected void setRawResult(T value) {
      this.result = value;
    }

    @Override
    public T getRawResult() {
      return result;
    }

    private synchronized T runDeserialization() {
      try {
        final Reader truncatedReader = reader.getReader().truncate(this.start, this.end);
        return partialDeserialize(truncatedReader, klass);
      } catch (IOException e) {
        throw new ActiveViamRuntimeException(e);
      }
    }

    /**
     * Deserializes a statistic from a reader into a given type.
     *
     * @param <T> The type of the statistic.
     * @param partialReader the {@link IMonitoringStatistic} to deserialize.
     * @param klass the type of {@link IMonitoringStatistic}.
     * @return the deserialized statistic
     */
    public <T extends IMonitoringStatistic> T partialDeserialize(
        final Reader partialReader, final Class<T> klass) {
      final ObjectReader reader =
          JacksonSerializer.getObjectMapper()
              .readerFor(klass)
              .withAttribute(DeserializerTask.COMPUTED_SUBTASKS_ATTRIBUTE, children)
              .withAttribute(DeserializerTask.CURRENT_TASK_POSITION_ATTRIBUTE, this.start);
      try {
        return reader.readValue(partialReader);
      } catch (IOException e) {
        throw new ActiveViamRuntimeException(e);
      }
    }
  }

  private static <T extends IMonitoringStatistic> DeserializerTask<T> generateTasks(
      MemoryStatisticDeserializerHelper helper,
      ForkJoinPool forkJoinPool,
      Class<T> klass,
      long taskSize,
      ReadersHandler readersHandler) {
    //
    // We use the structure from helper to find the least deep coverage of the file with objects of
    // size at  least taskSize
    //
    // The algorithm is the following :
    // Iterating on helper from the largest depth to the most shallow
    // If there are range entries at the level :
    //                - It should NOT be full covered by any of the existing tasks
    //                - Find any saved task at the previous level(or any level)? that is covered by
    // the current
    //                range, sum their sizes : if the range-sum of children > tasksize , save as
    // valid task that has the summed children as parent completer
    // Do this until you reach the depth 1

    final int maxDepth =
        helper.rangesPerDepth.keySet().stream()
            .max(Comparator.naturalOrder())
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "The deserialization helper is expected to have a non-empty map of range"));

    List<DeserializerTask<T>> list = new ArrayList<>();
    final DeserializerTask<T> headTask =
        new DeserializerTask<>(
            null, integer -> forkJoinPool, klass, readersHandler, 1, 0, helper.fileLength);

    for (int i = maxDepth; i > 0; --i) {
      List<Pair<Long, Long>> objectsList = helper.rangesPerDepth.get(i);
      int finalI = i;
      // First trim the list to remove the amount of tasks under the tasksize threshold
      final List<Pair<Long, Long>> trimmedList = objectsList.parallelStream()
              .filter(range -> range.getRight()-range.getLeft() > taskSize)
              .collect(Collectors.toList());
      trimmedList.forEach(
          pair -> {
            long taskLength = pair.getRight() - pair.getLeft();
            assert taskLength > 0;
            // Find covering tasks in list at depth i+1 :
            Collection<DeserializerTask<T>> coveredSubTasks =
                list.stream()
                    //						.filter(task -> task.depth == finalI + 1)
                    .filter(task -> task.end < pair.getRight() && pair.getLeft() < task.start)
                    .collect(Collectors.toList());
            final long sumOfSubTasks =
                coveredSubTasks.stream().mapToLong(task -> task.end - task.start).sum();

            if (taskLength - sumOfSubTasks > taskSize) {
              DeserializerTask<T> newTask =
                  new DeserializerTask<>(
                      headTask,
                      integer -> forkJoinPool,
                      klass,
                      readersHandler,
                      finalI,
                      pair.getLeft(),
                      pair.getRight());

              coveredSubTasks.forEach(
                  subtask -> {
                    subtask.addParent(newTask);
                    newTask.addChild(subtask);
                  });
              list.add(newTask);
            }
          });
    }
    list.forEach(headTask::addChild);
    return headTask;
  }

  /**
   * Asynchronously deserializes a statistic from a reader into a given type.
   *
   * @param <T> The type of the statistic.
   * @param file the file to load.
   * @param klass the type of {@link IMonitoringStatistic}.
   * @param forkJoinPool the {@link ForkJoinPool} that will handle parallelization of the tasks
   * @return the deserialized statistic
   */
  public static <T extends IMonitoringStatistic> T deserializeAsync(
      final File file, final Class<T> klass, final ForkJoinPool forkJoinPool) throws IOException {

    // Pre-read the file to determine whether a parallel deserialization is needed or not.
    // Not available on snappy files
    final boolean isCompressedFile = file.getName().endsWith("." + COMPRESSED_FILE_EXTENSION);
    InputStream inputStream = new FileInputStream(file);
    if (isCompressedFile) {
      // TODO : logger.warn if file is very big : -> unsnappy into parallel load
      // Single-Threaded parsing
      inputStream = new SnappyFramedInputStream(inputStream);
      try (final InputStreamReader reader =
          new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
        return deserialize(reader, klass);
      }
    } else {
      final MemoryStatisticDeserializerHelper helper;
      try (final InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
        helper = JacksonSerializer.getObjectMapper().readerFor(MemoryStatisticDeserializerHelper.class).readValue(new BufferedReader(streamReader));
        return doDeserializeAsync(file, klass, helper, forkJoinPool);
      } catch (IOException e) {
        throw new ActiveViamRuntimeException(e);
      }
    }
  }

  /**
   * Dumps the given statistics in a compressed file, adding the exportDate to the statistic.
   *
   * @param stat The stat to enrich with the export date and to dump.
   * @param directory Where to dump the file.
   * @param name The name of the file (without the extension)
   * @throws IOException if the file cannot be written
   */
  public static void writeStatisticFile(
      final IMemoryStatistic stat, final Path directory, final String name) throws IOException {
    final String fileName =
        name
            + "."
            + MemoryStatisticSerializerUtil.JSON_FILE_EXTENSION
            + "."
            + MemoryStatisticSerializerUtil.COMPRESSED_FILE_EXTENSION;

    try (final FileOutputStream fos = new FileOutputStream(directory.resolve(fileName).toFile());
        final SnappyFramedOutputStream compressorOS =
            new SnappyFramedOutputStream(fos);
        final OutputStreamWriter writer =
            new OutputStreamWriter(compressorOS, StandardCharsets.UTF_8)) {
      MemoryStatisticSerializerUtil.serialize(stat, writer);
      writer.flush();
    }
  }

  /**
   * Reads the file for statistics.
   *
   * @param file c.
   * @return the {@link IMemoryStatistic} contains in the file
   * @throws IOException If an I/O error occurs
   */
  public static IMemoryStatistic readStatisticFile(final File file) throws IOException {
    return deserializeAsync(file, IMemoryStatistic.class, ForkJoinPool.commonPool());
  }
}
