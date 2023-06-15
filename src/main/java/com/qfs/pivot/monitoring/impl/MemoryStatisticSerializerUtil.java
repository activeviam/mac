/*
 * (C) ActiveViam 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.qfs.pivot.monitoring.impl;

import com.activeviam.fwk.ActiveViamRuntimeException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.qfs.concurrent.cancellable.impl.CancellableCountedCompleter;
import com.qfs.concurrent.cancellable.impl.MultiCancellableCountedCompleter;
import com.qfs.jackson.impl.JacksonSerializer;
import com.qfs.monitoring.statistic.IMonitoringStatistic;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.pool.IThreadPoolSelector;
import com.quartetfs.fwk.impl.Pair;
import com.quartetfs.fwk.serialization.SerializerException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountedCompleter;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import lombok.Data;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorInputStream;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorOutputStream;

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

	private MemoryStatisticSerializerUtil() {
	}

	static {
		final SimpleModule deserializeModule = new SimpleModule();
		deserializeModule.addDeserializer(IMonitoringStatistic.class, new MonitoringStatisticDeserializer());
		deserializeModule.addDeserializer(IMemoryStatistic.class, new MemoryStatisticDeserializer());
		deserializeModule.addDeserializer(MemoryStatisticDeserializerHelper.class,
				new MemoryStatisticDeserializerHelperDeserializer());

		JacksonSerializer.getObjectMapper().registerModule(deserializeModule);
	}

	/**
	 * Serializes a statistic into a given writer.
	 *
	 * @param activeStatistic the {@link IMonitoringStatistic} to serialize.
	 * @param writer          the writer in which the data will be serialized.
	 */
	public static void serialize(IMonitoringStatistic activeStatistic, Writer writer) {
		try {
			JacksonSerializer.serialize(new MonitoringStatisticAdapter(activeStatistic),
					new BufferedWriter(writer));
		} catch (SerializerException e) {
			throw new ActiveViamRuntimeException(e);
		}
	}

	/**
	 * Serializes a statistic into a given writer.
	 *
	 * @param statistic the {@link IMemoryStatistic} to serialize.
	 * @param writer    the writer in which the data will be serialized.
	 */
	public static void serialize(IMemoryStatistic statistic, Writer writer) {
		try {
			JacksonSerializer.serialize(statistic.accept(new SerializerVisitor()), new BufferedWriter(writer));
		} catch (SerializerException e) {
			throw new ActiveViamRuntimeException(e);
		}
	}

	/**
	 * Deserializes a statistic from a reader into a given type.
	 *
	 * @param <T>          The type of the statistic.
	 * @param streamReader the {@link IMonitoringStatistic} to deserialize.
	 * @param klass        the type of {@link IMonitoringStatistic}.
	 * @return the deserialized statistic
	 */
	public static <T extends IMonitoringStatistic> T deserialize(final Reader streamReader, final Class<T> klass) {
		final ObjectReader reader = JacksonSerializer.getObjectMapper().readerFor(klass);
		try {
			return reader.readValue(new BufferedReader(streamReader));
		} catch (IOException e) {
			throw new ActiveViamRuntimeException(e);
		}
	}

	@Data
	public static class MemoryStatisticDeserializerHelper {

		public static final long PARALLEL_THRESHOLD = 100_000L;
		protected final Map<Integer, List<Pair<Long, Long>>> rangesPerDepth;
		protected final long fileLength;
	}

	private static <T extends IMonitoringStatistic> T doDeserializeAsync(
			final File file,
			final Class<T> klass,
			final MemoryStatisticDeserializerHelper helper,
			final ForkJoinPool forkJoinPool) throws IOException {

		final boolean isCompressedFile = file.getName().endsWith("." + COMPRESSED_FILE_EXTENSION);
		System.out.println();
		System.out.println(file.getName());
		//Only run parallel treatment  if the file length is below MemoryStatisticDeserializerHelper.PARALLEL_THRESHOLD
		if (helper.fileLength >= MemoryStatisticDeserializerHelper.PARALLEL_THRESHOLD) {
			final long taskSize = MemoryStatisticDeserializerHelper.PARALLEL_THRESHOLD/forkJoinPool.getParallelism();
			//Try to generate deserialization tasks with a length as close to taskSize that will cover the entire file
			DeserializerTask<T> headTask = generateTasks(file,helper,forkJoinPool,taskSize);
			headTask.getChildren().forEach(task -> System.out.println(task.getPendingCount()));
			headTask.getChildren().forEach(forkJoinPool::invoke);
			T val = forkJoinPool.invoke(headTask);
			headTask.getChildren().forEach(task -> System.out.println(task.getPendingCount()));

			return val;
		} else {
			//Single-Threaded parsing
			InputStream inputStream = new FileInputStream(file);
			if (isCompressedFile) {
				inputStream = new FramedSnappyCompressorInputStream(inputStream);
			}

			try (final InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
				return deserialize(reader, klass);
			}
		}
	}

	public static class DeserializerTask<T> extends MultiCancellableCountedCompleter<T> {
		final int depth;
		final long start;
		final long end;

		@Override public String toString() {
			return "DeserializerTask{" + "depth=" + depth + ", start=" + start + ", end=" + end + '}';
		}

		protected DeserializerTask(
				CancellableCountedCompleter<T> completer, IThreadPoolSelector poolSelector, int depth, long start,
				long end) {

			super(completer, poolSelector);
			this.depth = depth;
			this.start = start;
			this.end = end;
		}

		@Override protected void onRegularCompletion(CountedCompleter<?> caller) throws Throwable {
			super.onRegularCompletion(caller);
			System.out.println("I'm done with my children : "+ this);
		}

		@Override protected void computeSafely() throws Throwable {
			System.out.println("Well hello there -> "+ this);
		}
	}
	private static <T extends IMonitoringStatistic> DeserializerTask<T> generateTasks(
			File file,
			MemoryStatisticDeserializerHelper helper,
			ForkJoinPool forkJoinPool,
			long taskSize) {
		//
		// We use the structure from helper to find the least deep coverage of the file with objects of size at  least taskSize
		//
		// The algorithm is the following :
		// Iterating on helper from the largest depth to the most shallow
		// If there are range entries at the level :
		//                - It should NOT be full covered by any of the existing tasks
		//                - Find any saved task at the previous level(or any level)? that is covered by the current
		//                range, sum their sizes : if the range-sum of children > tasksize , save as valid task that has the summed children as parent completer
		//Do this until you reach the depth 1

		final int maxDepth = helper.rangesPerDepth.keySet()
				.stream()
				.max(Comparator.naturalOrder())
				.orElseThrow(() -> new IllegalStateException(
						"The deserialization helper is expected to have a non-empty map of range"));

		List<DeserializerTask<T>> list = new ArrayList<>();

		final DeserializerTask<T> headTask = new DeserializerTask<>(null, integer -> forkJoinPool, 1, 0, helper.fileLength);

		for (int i = maxDepth; i > 0; --i) {
			List<Pair<Long, Long>> objectsList = helper.rangesPerDepth.get(i);
			int finalI = i;
			objectsList.forEach(pair -> {
				long taskLength = pair.getRight() - pair.getLeft();
				assert taskLength > 0;
				// Find covering tasks in list at depth i+1 :
				Collection<DeserializerTask<T>> coveredSubTasks = list.stream()
						//.filter(task -> task.depth == finalI + 1)
						.filter(task -> task.end < pair.getRight() && pair.getLeft() < task.start)
						.collect(Collectors.toList());
				final long sumOfSubTasks = coveredSubTasks.stream().mapToLong(task -> task.end-task.start).sum();

				if (taskLength-sumOfSubTasks>taskSize){
					DeserializerTask<T> newTask = new DeserializerTask<>(headTask,
							integer -> forkJoinPool,
							finalI,
							pair.getLeft(),
							pair.getRight());

					coveredSubTasks.forEach(newTask::addChild);
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
	 * @param <T>          The type of the statistic.
	 * @param file         the file to load.
	 * @param klass        the type of {@link IMonitoringStatistic}.
	 * @param forkJoinPool the {@link ForkJoinPool} that will handle parallelization of the tasks
	 * @return the deserialized statistic
	 */
	public static <T extends IMonitoringStatistic> T deserializeAsync(
			final File file,
			final Class<T> klass,
			final ForkJoinPool forkJoinPool) throws IOException {

		//Pre-read the file to determine whether a parallel deserialization is needed or not
		final boolean isCompressedFile = file.getName().endsWith("." + COMPRESSED_FILE_EXTENSION);
		InputStream inputStream = new FileInputStream(file);
		if (isCompressedFile) {
			inputStream = new FramedSnappyCompressorInputStream(inputStream);
		}
		final MemoryStatisticDeserializerHelper helper;
		try (final InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
			helper = JacksonSerializer.getObjectMapper()
					.readerFor(MemoryStatisticDeserializerHelper.class)
					.readValue(new BufferedReader(streamReader));
			return doDeserializeAsync(file, klass, helper, forkJoinPool);
		} catch (IOException e) {
			throw new ActiveViamRuntimeException(e);
		}
	}

	/**
	 * Dumps the given statistics in a compressed file, adding the exportDate to the statistic.
	 *
	 * @param stat      The stat to enrich with the export date and to dump.
	 * @param directory Where to dump the file.
	 * @param name      The name of the file (without the extension)
	 * @throws IOException if the file cannot be written
	 */
	public static void writeStatisticFile(final IMemoryStatistic stat, final Path directory, final String name)
			throws IOException {
		final String fileName = name + "." + MemoryStatisticSerializerUtil.JSON_FILE_EXTENSION + "."
				+ MemoryStatisticSerializerUtil.COMPRESSED_FILE_EXTENSION;

		try (final FileOutputStream fos = new FileOutputStream(directory.resolve(fileName).toFile());
				final FramedSnappyCompressorOutputStream compressorOS = new FramedSnappyCompressorOutputStream(fos);
				final OutputStreamWriter writer = new OutputStreamWriter(compressorOS, StandardCharsets.UTF_8)) {
			MemoryStatisticSerializerUtil.serialize(stat, writer);
			writer.flush();
			compressorOS.finish();
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
