package at.hadl.logstatistics;

import at.hadl.logstatistics.analysis.QueryShapeFrequencyCounter;
import at.hadl.logstatistics.utils.BatchLogIterator;
import at.hadl.logstatistics.utils.preprocessing.WikidataPreprocessor;

import java.nio.file.Paths;

public class Application {

	public static void main(String[] args) throws Exception {
		try (var logBatches = new BatchLogIterator(Paths.get(System.getProperty("user.home"), "Desktop", "access-logs", "2018-02-26_2018-03-25_organic.tsv.gz"), BatchLogIterator.Compression.GZIP, 10000, 1)) {
			new QueryShapeFrequencyCounter(logBatches, Paths.get("queryshapes_wikidata_organic_7.tsv"), Paths.get("wikidata_predicate_map.txt"))
					.withPreprocessor(new WikidataPreprocessor())
					.withPredicateMap(Paths.get("wikidata_predicate_map.txt"))
					.startAnalysis();
		}
	}
}