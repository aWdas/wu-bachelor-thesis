package at.hadl.logstatistics.analysis;

import at.hadl.logstatistics.LabeledEdge;
import at.hadl.logstatistics.utils.BatchLogIterator;
import at.hadl.logstatistics.utils.GraphBuilder;
import at.hadl.logstatistics.utils.Preprocessing;
import at.hadl.logstatistics.utils.QueryParser;
import com.google.common.collect.Sets;
import org.apache.jena.query.Query;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class QueryShapeFrequencyCounter {
	private int maxStarShapeSize;
	private BatchLogIterator logBatches;
	private Path outFile;

	public QueryShapeFrequencyCounter(int maxStarShapeSize, BatchLogIterator logBatches, Path outFile) {
		this.maxStarShapeSize = maxStarShapeSize;
		this.logBatches = logBatches;
		this.outFile = outFile;
	}

	public void startAnalysis() throws IOException {
		ZonedDateTime start = ZonedDateTime.now();
		List<Map.Entry<Set<String>, Long>> allFrequencies = new ArrayList<>();
		AtomicLong totalLines = new AtomicLong(0);
		AtomicLong totalQueries = new AtomicLong(0);
		AtomicLong validQueries = new AtomicLong(0);

		while (logBatches.hasNext()) {
			var batch = logBatches.next();

			totalLines.getAndAdd(batch.size());

			var extractedQueries = batch.stream()
					.parallel()
					.flatMap(line -> Preprocessing.extractQueryString(line).stream())
					.collect(Collectors.toList());
			totalQueries.getAndAdd(extractedQueries.size());

			var parsedQueries = extractedQueries.stream().parallel().map(Preprocessing::preprocessVirtuosoQueryString).flatMap(queryString -> QueryParser.parseQuery(queryString).stream()).collect(Collectors.toList());
			validQueries.getAndAdd(parsedQueries.size());

			var predicateGroupFrequencies = new ArrayList<>(parsedQueries.stream()
					.parallel()
					.flatMap(this::extractStarShapePredicateCombinations)
					.collect(Collectors.groupingByConcurrent(predicateSet -> predicateSet, Collectors.counting()))
					.entrySet());

			allFrequencies.addAll(predicateGroupFrequencies);

			Duration executionDuration = Duration.between(start, ZonedDateTime.now());
			System.out.println("Batch completed!");
			System.out.println("Total lines: " + batch.size());
			System.out.println("Extracted queries: " + extractedQueries.size());
			System.out.println("Valid queries: " + parsedQueries.size());
			System.out.println("Total duration: " + executionDuration.toString());
			System.out.println();
		}

		var totalFrequencies = allFrequencies.stream()
				.collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.summingLong(Map.Entry::getValue)))
				.entrySet()
				.stream()
				.sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
				.collect(Collectors.toList());

		try (var fileWriter = new FileWriter(outFile.toFile())) {
			fileWriter.write("Total lines: " + totalLines.get() + " \n");
			fileWriter.write("Total queries: " + totalQueries.get() + " \n");
			fileWriter.write("Valid queries: " + validQueries.get() + " \n");

			totalFrequencies.forEach(entry -> {
				try {
					fileWriter.write(entry.getKey().toString() + "\t" + entry.getValue() + "\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
		}
	}

	private Stream<Set<String>> extractStarShapePredicateCombinations(Query query) {
		return GraphBuilder.constructGraphFromQuery(query)
				.map(queryGraph -> queryGraph.vertexSet().stream()
						.flatMap(vertex -> {
							var outgoingPredicates = queryGraph.outgoingEdgesOf(vertex).stream().map(LabeledEdge::getPredicate).collect(Collectors.toSet());
							Set<Set<String>> starCombinations = new HashSet<>();

							IntStream.rangeClosed(2, Math.min(maxStarShapeSize, outgoingPredicates.size())).forEach(size -> starCombinations.addAll(Sets.combinations(outgoingPredicates, size)));
							return starCombinations.stream();
						}))
				.orElse(Stream.empty());
	}
}
