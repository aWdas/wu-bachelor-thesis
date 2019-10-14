package at.hadl.logstatistics;

import at.hadl.logstatistics.analysis.QueryShapeFrequencyCounter;
import at.hadl.logstatistics.utils.PredicateMap;
import at.hadl.logstatistics.utils.io.BatchLogIterator;
import at.hadl.logstatistics.utils.preprocessing.DBPediaPreprocessor;
import at.hadl.logstatistics.utils.preprocessing.WikidataPreprocessor;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Application {

	public static void main(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(Option.builder("l").longOpt("logs").hasArgs().desc("The path(s) to one/multiple directory(/ies) of logs or single log file(s).").build());
		options.addOption(Option.builder("o").longOpt("outFiles").hasArgs().desc("The file to write the results into.").build());
		options.addOption("b", "batchSize", true, "The batch size for log processing.");
		options.addOption("po", "predicateMapOutFile", true, "The file to write the predicate map into.");
		options.addOption("pi", "predicateMapInFile", true, "The file to read the predicate map from.");
		options.addOption("pre", "preprocessor", true, "The preprocessor to use");
		options.addOption("skip", true, "Lines to skip in each file.");

		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = parser.parse(options, args);

		if (cmd.hasOption("l") && cmd.hasOption("o") && cmd.hasOption("po")) {
			if (cmd.getOptionValues("l").length != cmd.getOptionValues("o").length) {
				throw new RuntimeException("You must specify exactly as many output files as you specify log sources.");
			}

			PredicateMap predicateMap;
			if (cmd.hasOption("pi")) {
				System.out.println("Using predicate map from: " + cmd.getOptionValue("pi"));
				predicateMap = PredicateMap.fromPath(Paths.get(cmd.getOptionValue("pi"))).orElseThrow();
			} else {
				predicateMap = new PredicateMap();
			}

			var inputOutputPairs = IntStream.range(0, cmd.getOptionValues("l").length)
					.mapToObj(i -> new ImmutablePair<>(cmd.getOptionValues("l")[i], cmd.getOptionValues("o")[i]))
					.collect(Collectors.toList());

			inputOutputPairs.parallelStream()
					.forEach(inputOutputPair -> {
						var logPath = inputOutputPair.getLeft();
						var outFile = inputOutputPair.getRight();
						System.out.println("Analysing logs from " + logPath + " and writing results to " + outFile);

						try (var logBatches = new BatchLogIterator(Paths.get(logPath), BatchLogIterator.Compression.GZIP, Integer.parseInt(cmd.getOptionValue("b", "10000")), Integer.parseInt(cmd.getOptionValue("skip", "0")))) {
							var counter = new QueryShapeFrequencyCounter(logBatches, outFile)
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
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					});

			// Write the final predicate map to a file
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
