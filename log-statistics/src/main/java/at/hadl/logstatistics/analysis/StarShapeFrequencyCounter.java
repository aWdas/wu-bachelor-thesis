package at.hadl.logstatistics.analysis;

import at.hadl.logstatistics.LabeledEdge;
import at.hadl.logstatistics.utils.GraphBuilder;
import at.hadl.logstatistics.utils.PredicateMap;
import at.hadl.logstatistics.utils.Preprocessing;
import at.hadl.logstatistics.utils.QueryParser;
import com.google.common.collect.Sets;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class StarShapeFrequencyCounter {
	private int maxStarShapeSize;
	private Iterator<List<String>> logBatches;
	private Path outFile;

	public StarShapeFrequencyCounter(int maxStarShapeSize, Iterator<List<String>> logBatches, Path outFile) {
		this.maxStarShapeSize = maxStarShapeSize;
		this.logBatches = logBatches;
		this.outFile = outFile;
	}

	public void startAnalysis() throws IOException {
		ZonedDateTime start = ZonedDateTime.now();
		final ConcurrentHashMap<String, Integer> totalFrequencies = new ConcurrentHashMap<>();

		var predicateMap = new PredicateMap();


		LongAdder totalLines = new LongAdder();
		LongAdder totalQueries = new LongAdder();
		LongAdder validQueries = new LongAdder();
		LongAdder totalVertices = new LongAdder();

		while (logBatches.hasNext()) {
			var batch = logBatches.next();
			Collections.shuffle(batch);

			batch.parallelStream()
					.peek(line -> totalLines.increment())
					.flatMap(line -> Preprocessing.extractQueryString(line).stream())
					.peek(line -> totalQueries.increment())
					.map(Preprocessing::preprocessVirtuosoQueryString)
					.flatMap(queryString -> QueryParser.parseQuery(queryString).stream())
					.peek(query -> validQueries.increment())
					.flatMap(queryGraph -> GraphBuilder.constructGraphFromQuery(queryGraph, predicateMap).stream())
					.peek(queryGraph -> totalVertices.add(queryGraph.vertexSet().size()))
					.flatMap(this::extractStarShapePredicateCombinations)
					.forEach(queryShape -> totalFrequencies.compute(queryShape, (key, count) -> (count == null) ? 1 : count + 1));

			Duration executionDuration = Duration.between(start, ZonedDateTime.now());
			System.out.println("Batch complete!");
			System.out.println("Total duration: " + executionDuration.toString());
			System.out.println();
		}

		var sortedTotalFrequencies = totalFrequencies.entrySet().stream()
				.filter(entry -> entry.getValue() > 100)
				.sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
				.collect(Collectors.toList());

		System.out.println("PredicateMap size: " + predicateMap.size());

		try (var fileWriter = new FileWriter(outFile.toFile())) {
			fileWriter.write("TotalLines\t" + totalLines.sum() + " \n");
			fileWriter.write("TotalQueries\t" + totalQueries.sum() + " \n");
			fileWriter.write("ValidQueries\t" + validQueries.sum() + " \n");
			fileWriter.write("TotalVertices\t" + totalVertices.sum() + " \n");

			sortedTotalFrequencies.forEach(entry -> {
				try {
					var entryString = Arrays.stream(entry.getKey().split(","))
							.map(Integer::parseInt)
							.map(predicateMap::getPredicateForInt)
							.collect(Collectors.joining(","));

					fileWriter.write(entryString + "\t" + entry.getValue() + "\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
		}
	}

	private Stream<String> extractStarShapePredicateCombinations(DefaultDirectedGraph<String, LabeledEdge> queryGraph) {
		return queryGraph.vertexSet().stream()
				.filter(vertex -> queryGraph.outgoingEdgesOf(vertex).size() <= maxStarShapeSize)
				.flatMap(vertex -> {
					var outgoingPredicates = queryGraph.outgoingEdgesOf(vertex).stream().map(LabeledEdge::getPredicate).collect(Collectors.toSet());
					Set<String> starCombinations = new HashSet<>();

					IntStream.rangeClosed(1, Math.min(maxStarShapeSize, outgoingPredicates.size()))
							.forEach(size -> {
								var combinationStrings = Sets.combinations(outgoingPredicates, size).stream()
										.map(combination -> combination.stream().map(Object::toString).collect(Collectors.joining(",")))
										.collect(Collectors.toList());
								starCombinations.addAll(combinationStrings);
							});
					return starCombinations.stream();
				});
	}
}
