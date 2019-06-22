package at.hadl.logstatistics.utils;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class BatchLogIterator implements Iterator<List<String>>, AutoCloseable {
	private Compression compression;
	private int batchSize;
	private Iterator<Path> files;
	private BufferedReader currentFileReader;
	private boolean lastFileFinished;

	public BatchLogIterator(Path directoryPath, Compression compression, int batchSize) throws IOException {
		this.compression = compression;
		this.batchSize = batchSize;
		this.files = Files.walk(directoryPath)
				.filter(Files::isRegularFile)
				.iterator();

		if (!files.hasNext()) {
			currentFileReader = new BufferedReader(new InputStreamReader(InputStream.nullInputStream()));
			lastFileFinished = true;
		} else {
			currentFileReader = getReaderForFile(files.next());
			lastFileFinished = false;
		}

	}

	@Override
	public boolean hasNext() {
		return files.hasNext() || !lastFileFinished;
	}

	@Override
	public List<String> next() {
		var lines = new ArrayList<String>();

		int i = 0;

		while (i < batchSize) {
			try {
				String line = currentFileReader.readLine();
				if (line == null) {
					currentFileReader.close();
					if (files.hasNext()) {
						var path = files.next();
						System.out.println("Switching to file: " + path.toString());
						currentFileReader = getReaderForFile(path);
					} else {
						lastFileFinished = true;
						break;
					}
				} else {
					lines.add(line);
					i++;
				}
			} catch (IOException e) {
				throw new RuntimeException("An error occurred during log file reading: ", e);
			}
		}

		return lines;
	}

	private BufferedReader getReaderForFile(Path path) throws IOException {
		if (compression.equals(Compression.BZIP2)) {
			return new BufferedReader(new InputStreamReader(new BZip2CompressorInputStream(new FileInputStream(path.toFile()), true)));
		} else if (compression.equals(Compression.GZIP)) {
			return new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(path.toFile()))));
		} else {
			return new BufferedReader(new InputStreamReader(new FileInputStream(path.toFile())));
		}
	}

	@Override
	public void close() throws Exception {
		currentFileReader.close();
		lastFileFinished = true;
		files = Collections.emptyIterator();
	}

	public enum Compression {
		GZIP,
		BZIP2
	}
}