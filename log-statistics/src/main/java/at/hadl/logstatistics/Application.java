package at.hadl.logstatistics;

import at.hadl.logstatistics.analysis.QueryShapeFrequencyCounter;
import at.hadl.logstatistics.utils.BatchLogIterator;

import java.nio.file.Paths;

public class Application {

	public static void main(String[] args) throws Exception {
		try (var logBatches = new BatchLogIterator(Paths.get(System.getProperty("user.home"), "Desktop", "access-logs", "usewod-2016"), BatchLogIterator.Compression.GZIP, 10000)) {
			new QueryShapeFrequencyCounter(15, logBatches, Paths.get("queryshapes2016_3.txt")).startAnalysis();
		}
	}
}