package at.hadl.logstatistics;

import at.hadl.logstatistics.analysis.QueryShapeFrequencyCounter;
import at.hadl.logstatistics.utils.BatchLogIterator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

import java.nio.file.Paths;

public class Application {

	public static void main(String[] args) throws Exception {
		Options options = new Options();
		options.addOption("l", "logDirectory", true, "The directory where the log files lie.");
		options.addOption("o", "outFile", true, "The file to write the results into.");
		options.addOption("b", "batchSize", true, "The batch size for log processing.");
		options.addOption("s", "starShapeSize", true, "The maximum star shape size analyzed.");

		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = parser.parse(options, args);

		if(cmd.hasOption("l") && cmd.hasOption("o")) {
			try (var logBatches = new BatchLogIterator(Paths.get(cmd.getOptionValue("l")), BatchLogIterator.Compression.GZIP, Integer.parseInt(cmd.getOptionValue("b", "5000")))) {
				new QueryShapeFrequencyCounter(Integer.parseInt(cmd.getOptionValue("s", "6")), logBatches, Paths.get(cmd.getOptionValue("o"))).startAnalysis();
			}
		} else {
			throw new RuntimeException("l and o are mandatory parameters!");
		}
	}
}