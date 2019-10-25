/*
 * (C) ActiveViam 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.qfs.pivot.monitoring.impl;

import com.activeviam.mac.Workaround;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.qfs.jackson.impl.JacksonSerializer;
import com.qfs.monitoring.statistic.IMonitoringStatistic;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.quartetfs.fwk.QuartetRuntimeException;
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
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorInputStream;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorOutputStream;

/**
 * Utility class to serialize/deserialize {@link IMemoryStatistic}s.
 *
 * @author ActiveViam
 */
@Workaround(jira = "PIVOT-4093", solution = "Waiting for the next version with the fix")
public class MemoryStatisticSerializerUtil {

	/** Extension of the files that contain the dumped memory statistics. */
	public static final String JSON_FILE_EXTENSION = "json";

	/** Extension of the compressed files. */
	public static final String COMPRESSED_FILE_EXTENSION = "sz";

	private MemoryStatisticSerializerUtil() {}

	static {
		final SimpleModule deserializeModule = new SimpleModule();
		deserializeModule.addDeserializer(
				IMonitoringStatistic.class,
				new MonitoringStatisticDeserializer());
		deserializeModule.addDeserializer(
				IMemoryStatistic.class,
				new MemoryStatisticDeserializer());
		JacksonSerializer.getObjectMapper().registerModule(deserializeModule);
	}

	/**
	 * Serializes a statistic into a given writer.
	 * @param activeStatistic the {@link IMonitoringStatistic} to serialize.
	 * @param writer the writer in which the data will be serialized.
	 */
	public static void serialize(IMonitoringStatistic activeStatistic, Writer writer) {
		try {
			JacksonSerializer.serialize(
					new MonitoringStatisticAdapter(activeStatistic),
					new BufferedWriter(writer));
		} catch (SerializerException e) {
			throw new QuartetRuntimeException(e);
		}
	}

	/**
	 * Serializes a statistic into a given writer.
	 * @param statistic the {@link IMemoryStatistic} to serialize.
	 * @param writer the writer in which the data will be serialized.
	 */
	public static void serialize(IMemoryStatistic statistic, Writer writer) {
		try {
			JacksonSerializer.serialize(
					statistic.accept(new SerializerVisitor()),
					new BufferedWriter(writer));
		} catch (SerializerException e) {
			throw new QuartetRuntimeException(e);
		}
	}

	/**
	 * Deserializes a statistic from a reader into a given type.
	 *
	 * @param <T> The type of the statistic.
	 * @param statistic the {@link IMonitoringStatistic} to deserialize.
	 * @param klass the type of {@link IMonitoringStatistic}.
	 * @return the deserialized statistic
	 */
	public static <T extends IMonitoringStatistic> T deserialize(
			final Reader statistic,
			final Class<T> klass) {
		ObjectReader reader = JacksonSerializer.getObjectMapper().readerFor(klass);
		try {
			return reader.readValue(new BufferedReader(statistic));
		} catch (IOException e) {
			throw new QuartetRuntimeException(e);
		}
	}

	/**
	 * Dumps the given statistics in a compressed file, adding the exportDate to the statistic.
	 * @param stat The stat to enrich with the export date and to dump.
	 * @param directory Where to dump the file.
	 * @param name The name of the file (without the extension)
	 *
	 * @throws IOException if the file cannot be written
	 */
	public static void writeStatisticFile(
			final IMemoryStatistic stat,
			final Path directory,
			final String name) throws IOException {
		final String fileName = name + "." + MemoryStatisticSerializerUtil.JSON_FILE_EXTENSION
				+ "." + MemoryStatisticSerializerUtil.COMPRESSED_FILE_EXTENSION;

		try (
				final FileOutputStream fos =
						new FileOutputStream(directory.resolve(fileName).toFile());
				final FramedSnappyCompressorOutputStream compressorOS =
						new FramedSnappyCompressorOutputStream(fos);
				final OutputStreamWriter writer = new OutputStreamWriter(
						compressorOS,
						StandardCharsets.UTF_8)) {
			MemoryStatisticSerializerUtil.serialize(stat, writer);
			writer.flush();
			compressorOS.finish();
		}
	}

	/**
	 * Reads the file for statistics.
	 * @param file the file to load.
	 * @return the {@link IMemoryStatistic} contains in the file
	 * @throws IOException If an I/O error occurs
	 */
	public static IMemoryStatistic readStatisticFile(final File file) throws IOException {
		final boolean isCompressedFile = file.getName().endsWith("." + COMPRESSED_FILE_EXTENSION);

		InputStream inputStream = new FileInputStream(file);
		if (isCompressedFile) {
			inputStream = new FramedSnappyCompressorInputStream(inputStream);
		}

		try (final InputStreamReader reader =
				new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
			return deserialize(reader, IMemoryStatistic.class);
		}
	}
}
