package at.hadl.logstatistics.analysis;

import at.hadl.logstatistics.utils.BatchLogIterator;
import at.hadl.logstatistics.utils.Preprocessing;
import at.hadl.logstatistics.utils.QueryParser;
import org.apache.jena.query.QueryException;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InvalidPredicateCounter {
	private BatchLogIterator logBatches;
	private Path outFile;

	public InvalidPredicateCounter(BatchLogIterator logBatches, Path outFile) {
		this.logBatches = logBatches;
		this.outFile = outFile;
	}

	public void startAnalysis() throws IOException {

		var undefinedPredicateCounts = new ArrayList<Map.Entry<String, Long>>();
		while (logBatches.hasNext()) {
			var lines = logBatches.next();

			undefinedPredicateCounts.addAll(lines.stream()
					.parallel()
					.flatMap(line -> Preprocessing.extractQueryString(line).stream())
					.map(Preprocessing::removeVirtuosoPragmas)
					.map(Preprocessing::removeIncorrectCommas)
					.flatMap(this::getUndefinedPrefixes)
					.collect(Collectors.groupingByConcurrent(Function.identity(), Collectors.counting()))
					.entrySet());
		}

		var totalFrequencies = undefinedPredicateCounts.stream()
				.collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.summingLong(Map.Entry::getValue)))
				.entrySet()
				.stream()
				.sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
				.collect(Collectors.toList());

		try (var fileWriter = new FileWriter(outFile.toFile())) {
			totalFrequencies.forEach(entry -> {
				try {
					fileWriter.write(entry.getKey() + "\t" + entry.getValue() + "\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
		}
	}

	private Stream<String> getUndefinedPrefixes(String queryString) {
		var undefinedPrefixes = new ArrayList<String>();
		var noMoreUndefinedPrefixes = false;

		while (!noMoreUndefinedPrefixes) {
			try {
				QueryParser.parseQueryFailing(queryString);
				noMoreUndefinedPrefixes = true;
			} catch (QueryException e) {
				if (e.getMessage().contains("Unresolved prefixed name:")) {
					var prefix = e.getMessage().split("Unresolved prefixed name:")[1].split(":")[0];
					undefinedPrefixes.add(prefix);
					queryString = "PREFIX " + prefix + ": <http://xmlns.com/foaf/0.1/> \n" + queryString;
				} else {
					noMoreUndefinedPrefixes = true;
				}
			}
		}

		return undefinedPrefixes.stream();
	}
}
