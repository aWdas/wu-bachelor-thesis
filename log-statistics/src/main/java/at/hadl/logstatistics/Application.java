package at.hadl.logstatistics;

import at.hadl.logstatistics.analysis.QueryShapeFrequencyCounter;
import at.hadl.logstatistics.utils.io.BatchLogIterator;
import at.hadl.logstatistics.utils.preprocessing.DBPediaPreprocessor;
import at.hadl.logstatistics.utils.preprocessing.WikidataPreprocessor;
import org.apache.commons.cli.*;

import java.nio.file.Paths;

public class Application {

	public static void main(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(Option.builder("l").longOpt("logs").hasArgs().desc("The path(s) to one/multiple directory(/ies) of logs or single log file(s).").build());
		options.addOption(Option.builder("o").longOpt("outFiles").hasArgs().desc("The file to write the results into.").build());
		options.addOption("b", "batchSize", true, "The batch size for log processing.");
		options.addOption("po", "predicateMapOutFile", true, "The file to write the predicate map into.");
		options.addOption("pi", "predicateMapInFile", true, "The file to read the predicate map from.");
		options.addOption("pre", "preprocessor", true, "The preprocessor to use");

		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = parser.parse(options, args);

		if (cmd.hasOption("l") && cmd.hasOption("o") && cmd.hasOption("po")) {
			if (cmd.getOptionValues("l").length != cmd.getOptionValues("o").length) {
				throw new RuntimeException("You must specify exactly as many output files as you specify log sources.");
			}

			for (int i = 0; i < cmd.getOptionValues("l").length; i++) {
				String logPath = cmd.getOptionValues("l")[i];
				String outFilePath = cmd.getOptionValues("o")[i];

				System.out.println("Analysing logs from " + logPath + "and writing results to " + outFilePath);

				try (var logBatches = new BatchLogIterator(Paths.get(logPath), BatchLogIterator.Compression.GZIP, Integer.parseInt(cmd.getOptionValue("b", "10000")), 0)) {
					var counter = new QueryShapeFrequencyCounter(logBatches, Paths.get(outFilePath), Paths.get(cmd.getOptionValue("po")));

					if (cmd.hasOption("pi")) {
						System.out.println("Using predicate map from: " + cmd.getOptionValue("pi"));
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
			}
		} else {
			throw new RuntimeException("l, o, and po are mandatory parameters!");
		}
	}
}
