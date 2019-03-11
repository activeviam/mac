/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac;

import com.qfs.pivot.monitoring.impl.MemoryStatisticSerializerUtil;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorInputStream;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Objects;

/**
 * Tool class providing CLI methods.
 *
 * @author ActiveViam
 */
public class Tools {

	public static void main(final String[] args) {
//		if ("extract".equals(args[0])) {
			extractSnappyFile(args[0]);
//			return 0;
//		} else {
//			System.err.println("Unsupported command. Got " + Arrays.toString(args));
////			return 1;
//		}
	}

	public static void extractSnappyFile(final String path) {
		Objects.requireNonNull(path, "File to uncompress not provided");

		final String extension = MemoryStatisticSerializerUtil.COMPRESSED_FILE_EXTENSION;
		boolean isCompressedFile = path.endsWith("." + extension);
		if (!isCompressedFile) {
			throw new IllegalArgumentException(
					"Extension of `" + path + "` does not match the list of compressed extensions. Use one of [" + MemoryStatisticSerializerUtil.COMPRESSED_FILE_EXTENSION + "] to use this tool.");
		}

		final String subpart = path.substring(0, path.length() - extension.length() - 1);
		final Path uncompressedPath = Paths.get(subpart.endsWith(".json") ? subpart : subpart + ".json");
		final InputStream rawInputStream;
		try {
			rawInputStream = new FileInputStream(path);
		} catch (FileNotFoundException e) {
			throw new IllegalArgumentException(
					String.format("File `%s` does not exist", path),
					e);
		}
		final InputStream inputStream;
		try {
			inputStream = new FramedSnappyCompressorInputStream(rawInputStream);
		} catch (IOException e) {
			throw new RuntimeException(
					String.format("Cannot read `%s` as a Snappy file", path),
					e);
		}
		try {
			Files.copy(inputStream, uncompressedPath, StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			throw new RuntimeException(
					String.format("Failed to extract `%s` to `%s`", path, uncompressedPath),
					e);
		}
		System.err.printf("File `%s` extracted to `%s`%n", path, uncompressedPath);
	}

}
