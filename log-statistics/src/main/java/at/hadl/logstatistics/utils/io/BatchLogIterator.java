package at.hadl.logstatistics.utils.io;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public class BatchLogIterator implements Iterator<List<String>>, AutoCloseable {
	private final int skipLines;
	private Compression compression;
	private int batchSize;
	private Iterator<Path> files;
	private BufferedReader currentFileReader;
	private boolean lastFileFinished;

	public BatchLogIterator(Path path, Compression compression, int batchSize, int skipLines) throws IOException {
		this.compression = compression;
		this.batchSize = batchSize;
		this.skipLines = skipLines;
		if (Files.isDirectory(path)) {
			this.files = Files.walk(path)
					.filter(Files::isRegularFile)
					.iterator();
		} else if (Files.isRegularFile(path)) {
			this.files = Stream.of(path).iterator();
		} else {
			throw new RuntimeException("The given path must either be a directory or a regular file");
		}


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
		var lines = new ArrayList<String>(batchSize);

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
		BufferedReader reader;

		if (compression.equals(Compression.BZIP2)) {
			reader = new BufferedReader(new InputStreamReader(new BZip2CompressorInputStream(new FileInputStream(path.toFile()), true)), 100000);
		} else if (compression.equals(Compression.GZIP)) {
			reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(path.toFile()))), 100000);
		} else {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(path.toFile())));
		}

		for (int i = 0; i < skipLines; i++) {
			reader.readLine();
		}

		return reader;
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
