package at.hadl.logstatistics;

import at.hadl.logstatistics.analysis.StarShapeSizeCounter;
import at.hadl.logstatistics.utils.BatchLogIterator;

import java.nio.file.Paths;

public class Application {

	public static void main(String[] args) throws Exception {
		try (var logBatches = new BatchLogIterator(Paths.get(System.getProperty("user.home"), "Desktop", "access-logs", "usewod-2014"), BatchLogIterator.Compression.GZIP, 10000)) {
			new StarShapeSizeCounter(logBatches, Paths.get("shapes2014.txt")).startAnalysis();
		}
	}
}
