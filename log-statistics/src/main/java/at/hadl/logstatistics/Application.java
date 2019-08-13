package at.hadl.logstatistics;

import at.hadl.logstatistics.analysis.QueryShapeFrequencyCounter;
import at.hadl.logstatistics.utils.PredicateMap;
import at.hadl.logstatistics.utils.io.BatchLogIterator;
import at.hadl.logstatistics.utils.preprocessing.DBPediaPreprocessor;
import at.hadl.logstatistics.utils.preprocessing.WikidataPreprocessor;
import org.apache.commons.cli.*;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

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

			var predicateMap = new PredicateMap();
			if (cmd.hasOption("pi")) {
				System.out.println("Using predicate map from: " + cmd.getOptionValue("pi"));
				predicateMap = PredicateMap.fromPath(Paths.get(cmd.getOptionValue("pi"))).orElseThrow();
			}

			for (int i = 0; i < cmd.getOptionValues("l").length; i++) {
				String logPath = cmd.getOptionValues("l")[i];
				String outFilePath = cmd.getOptionValues("o")[i];

				System.out.println("Analysing logs from " + logPath + " and writing results to " + outFilePath);

				try (var logBatches = new BatchLogIterator(Paths.get(logPath), BatchLogIterator.Compression.GZIP, Integer.parseInt(cmd.getOptionValue("b", "10000")), 1)) {
					var counter = new QueryShapeFrequencyCounter(logBatches, Paths.get(outFilePath))
							.withPredicateMap(predicateMap);

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

			try (var fileWriter = new FileWriter(Paths.get(cmd.getOptionValue("po")).toFile())) {
				predicateMap.getPredicateMap().entrySet().stream()
						.sorted(Map.Entry.comparingByValue())
						.forEach(entry -> {
							try {
								fileWriter.write(entry.getKey() + "\t" + entry.getValue() + "\n");
							} catch (IOException e) {
								e.printStackTrace();
							}
						});
			}
		} else {
			throw new RuntimeException("l, o, and po are mandatory parameters!");
		}
	}
}
