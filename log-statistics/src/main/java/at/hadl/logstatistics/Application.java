package at.hadl.logstatistics;

import at.hadl.logstatistics.analysis.QueryShapeFrequencyCounter;
import at.hadl.logstatistics.utils.BatchLogIterator;
import at.hadl.logstatistics.utils.preprocessing.DBPediaPreprocessor;
import at.hadl.logstatistics.utils.preprocessing.WikidataPreprocessor;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

import java.nio.file.Paths;

public class Application {

	public static void main(String[] args) throws Exception {
		Options options = new Options();
		options.addOption("l", "logs", true, "The path to a directory of logs or a single log file.");
		options.addOption("o", "outFile", true, "The file to write the results into.");
		options.addOption("b", "batchSize", true, "The batch size for log processing.");
		options.addOption("po", "predicateMapOutFile", true, "The file to write the predicate map into.");
		options.addOption("pi", "predicateMapInFile", true, "The file to read the predicate map from.");
		options.addOption("pre", "preprocessor", true, "The preprocessor to use");

		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = parser.parse(options, args);

		if (cmd.hasOption("l") && cmd.hasOption("o") && cmd.hasOption("po")) {
			try (var logBatches = new BatchLogIterator(Paths.get(cmd.getOptionValue("l")), BatchLogIterator.Compression.GZIP, Integer.parseInt(cmd.getOptionValue("b", "10000")), 0)) {
				var counter = new QueryShapeFrequencyCounter(logBatches, Paths.get(cmd.getOptionValue("o")), Paths.get(cmd.getOptionValue("po")));

				if (cmd.hasOption("pi")) {
					counter = counter.withPredicateMap(Paths.get(cmd.getOptionValue("pi")));
				}

				if (cmd.hasOption("pre")) {
					if (cmd.getOptionValue("pre").equals("wikidata")) {
						counter = counter.withPreprocessor(new WikidataPreprocessor());
					} else if (cmd.getOptionValue("pre").equals("dbpedia")) {
						counter = counter.withPreprocessor(new DBPediaPreprocessor());
					} else {
						throw new RuntimeException("Option pre has an invalid/unknown value");
					}
				}

				counter.startAnalysis();
			}
		} else {
			throw new RuntimeException("l, o, and po are mandatory parameters!");
		}
	}
}