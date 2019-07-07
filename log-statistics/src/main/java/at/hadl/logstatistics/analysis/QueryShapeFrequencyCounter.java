package at.hadl.logstatistics.analysis;

import at.hadl.logstatistics.LabeledEdge;
import at.hadl.logstatistics.utils.GraphBuilder;
import at.hadl.logstatistics.utils.PredicateMap;
import at.hadl.logstatistics.utils.Preprocessing;
import at.hadl.logstatistics.utils.QueryParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public class QueryShapeFrequencyCounter {
	private int maxStarShapeSize;
	private Iterator<List<String>> logBatches;
	private Path outFile;

	public QueryShapeFrequencyCounter(int maxStarShapeSize, Iterator<List<String>> logBatches, Path outFile) {
		this.maxStarShapeSize = maxStarShapeSize;
		this.logBatches = logBatches;
		this.outFile = outFile;
	}

	public void startAnalysis() throws IOException {
		ZonedDateTime start = ZonedDateTime.now();
		final ConcurrentHashMap<Set<String>, Integer> totalFrequencies = new ConcurrentHashMap<>();
		Optional<ConcurrentHashMap<String, Integer>> predicateHashMapOptional = Optional.empty();
		try {
			ConcurrentHashMap<String, Integer> predicateHashMap = new ConcurrentHashMap<>();
			Files.lines(Path.of("predicate-map.txt")).forEach(line -> {
				var lineParts = line.split("\t");
				predicateHashMap.put(lineParts[0], Integer.parseInt(lineParts[1]));
			});
			predicateHashMapOptional = Optional.of(predicateHashMap);
		} catch (Exception e) {
			e.printStackTrace();
		}

		final PredicateMap predicateMap;
		predicateMap = predicateHashMapOptional.map(PredicateMap::new).orElseGet(PredicateMap::new);

		LongAdder totalLines = new LongAdder();
		LongAdder totalQueries = new LongAdder();
		LongAdder validQueries = new LongAdder();
		LongAdder variablePredicateQueries = new LongAdder();

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
					.flatMap(queryGraph -> GraphBuilder.constructGraphFromQuery(queryGraph, predicateMap, variablePredicateQueries).stream())
					.flatMap(query -> extractStarShapes(query).stream())
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

		ObjectMapper objectMapper = new ObjectMapper();

		try (var fileWriter = new FileWriter(outFile.toFile())) {
			fileWriter.write("TotalLines\t" + totalLines.sum() + " \n");
			fileWriter.write("TotalQueries\t" + totalQueries.sum() + " \n");
			fileWriter.write("ValidQueries\t" + validQueries.sum() + " \n");
			fileWriter.write("VariablePredicateQueries\t" + variablePredicateQueries.sum() + "\n");

			sortedTotalFrequencies.forEach(entry -> {
				try {
					fileWriter.write(objectMapper.writeValueAsString(entry.getKey()) + "\t" + entry.getValue() + "\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
		}

		try (var fileWriter = new FileWriter(Path.of("predicate-map.txt").toFile())) {
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
	}

	private Optional<Set<String>> extractStarShapes(DefaultDirectedGraph<String, LabeledEdge> queryGraph) {
		if (queryGraph.vertexSet().stream().anyMatch(vertex -> queryGraph.outgoingEdgesOf(vertex).size() > maxStarShapeSize)) {
			return Optional.empty();
		}
		return Optional.of(queryGraph.vertexSet().stream()
				.filter(vertex -> !queryGraph.outgoingEdgesOf(vertex).isEmpty())
				.map(vertex -> queryGraph.outgoingEdgesOf(vertex).stream()
						.map(LabeledEdge::getPredicate)
						.distinct()
						.sorted()
						.map(Object::toString)
						.collect(Collectors.joining(",")))
				.collect(Collectors.toSet()));
	}
}
