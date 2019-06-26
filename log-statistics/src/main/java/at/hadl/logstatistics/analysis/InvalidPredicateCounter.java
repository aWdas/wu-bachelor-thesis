package at.hadl.logstatistics.analysis;

import at.hadl.logstatistics.utils.Preprocessing;
import at.hadl.logstatistics.utils.QueryParser;
import com.google.common.base.CharMatcher;
import org.apache.jena.query.QueryException;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InvalidPredicateCounter {
	private Iterator<List<String>> logBatches;
	private Path outFile;
	private static CharMatcher ascii = CharMatcher.ascii();

	public InvalidPredicateCounter(Iterator<List<String>> logBatches, Path outFile) {
		this.logBatches = logBatches;
		this.outFile = outFile;
	}

	public void startAnalysis() throws IOException {
		ZonedDateTime start = ZonedDateTime.now();
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

			System.out.println("Batch complete!");
			Duration executionDuration = Duration.between(start, ZonedDateTime.now());
			System.out.println("Total duration: " + executionDuration.toString());
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
					var prefix = e.getMessage().split("Unresolved prefixed name:")[1].split(":")[0].trim();
					if (ascii.matchesAllOf(prefix)) {
						undefinedPrefixes.add(prefix);
						queryString = "PREFIX " + prefix + ": <http://xmlns.com/foaf/0.1/> \n" + queryString;
					} else {
						noMoreUndefinedPrefixes = true;
					}
				} else {
					noMoreUndefinedPrefixes = true;
				}
			}
		}

		return undefinedPrefixes.stream();
	}
}
