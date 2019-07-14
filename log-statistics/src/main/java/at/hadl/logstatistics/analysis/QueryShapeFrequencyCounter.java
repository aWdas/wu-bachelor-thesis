package at.hadl.logstatistics.analysis;

import at.hadl.logstatistics.LabeledEdge;
import at.hadl.logstatistics.utils.GraphBuilder;
import at.hadl.logstatistics.utils.PredicateMap;
import at.hadl.logstatistics.utils.QueryParser;
import at.hadl.logstatistics.utils.preprocessing.NoopPreprocessor;
import at.hadl.logstatistics.utils.preprocessing.Preprocessor;
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

public class QueryShapeFrequencyCounter {
	private Iterator<List<String>> logBatches;
	private Path outFile;
	private Path predicateMapOutPath;
	private PredicateMap predicateMap;
	private LongAdder totalLines = new LongAdder();
	private LongAdder totalQueries = new LongAdder();
	private LongAdder validQueries = new LongAdder();
	private LongAdder variablePredicateQueries = new LongAdder();
	private LongAdder subQueryQueries = new LongAdder();
	private LongAdder predicatePathQueries = new LongAdder();
	private Preprocessor preprocessor = new NoopPreprocessor();

	public QueryShapeFrequencyCounter(Iterator<List<String>> logBatches, Path outFile, Path predicateMapOutPath) {
		this.logBatches = logBatches;
		this.outFile = outFile;
		this.predicateMapOutPath = predicateMapOutPath;
		this.predicateMap = new PredicateMap();
	}

	public QueryShapeFrequencyCounter withPreprocessor(Preprocessor preprocessor) {
		this.preprocessor = preprocessor;
		return this;
	}

	public QueryShapeFrequencyCounter withPredicateMap(Path predicateMapPath) {
		this.predicateMap = PredicateMap.fromPath(predicateMapPath).orElseThrow();
		return this;
	}

	public void startAnalysis() throws IOException {
		ZonedDateTime start = ZonedDateTime.now();
		final ConcurrentHashMap<String, Integer> totalFrequencies = new ConcurrentHashMap<>();

		while (logBatches.hasNext()) {
			var batch = logBatches.next();
			Collections.shuffle(batch);

			batch.parallelStream()
					.peek(line -> totalLines.increment())
					.flatMap(line -> preprocessor.extractQueryString(line).stream())
					.peek(line -> totalQueries.increment())
					.map(preprocessor::preprocessQueryString)
					.flatMap(queryString -> QueryParser.parseQuery(queryString).stream())
					.peek(query -> validQueries.increment())
					.flatMap(queryGraph -> GraphBuilder.constructGraphFromQuery(queryGraph, predicateMap, variablePredicateQueries, subQueryQueries, predicatePathQueries).stream())
					.flatMap(query -> extractStarShapes(query).stream())
					.forEach(queryShape -> totalFrequencies.compute(queryShape, (key, count) -> (count == null) ? 1 : count + 1));

			Duration executionDuration = Duration.between(start, ZonedDateTime.now());
			System.out.println("Batch complete!");
			System.out.println("Total duration: " + executionDuration.toString());
			System.out.println();
		}

		var sortedTotalFrequencies = totalFrequencies.entrySet().stream()
				.sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
				.collect(Collectors.toList());

		System.out.println("PredicateMap size: " + predicateMap.size());

		writeResults(sortedTotalFrequencies);
	}

	private void writeResults(List<Map.Entry<String, Integer>> sortedTotalFrequencies) throws IOException {
		try (var fileWriter = new FileWriter(outFile.toFile())) {
			fileWriter.write("TotalLines\t" + totalLines.sum() + " \n");
			fileWriter.write("TotalQueries\t" + totalQueries.sum() + " \n");
			fileWriter.write("ValidQueries\t" + validQueries.sum() + " \n");
			fileWriter.write("VariablePredicateQueries\t" + variablePredicateQueries.sum() + "\n");
			fileWriter.write("SubQueryQueries\t" + subQueryQueries.sum() + "\n");
			fileWriter.write("PredicatePathQueries\t" + predicatePathQueries.sum() + "\n");
			fileWriter.write("query_shape\tcount\n");
			sortedTotalFrequencies.forEach(entry -> {
				try {
					fileWriter.write(entry.getKey() + "\t" + entry.getValue() + "\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
		}

		try (var fileWriter = new FileWriter(predicateMapOutPath.toFile())) {
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

	private Optional<String> extractStarShapes(DefaultDirectedGraph<String, LabeledEdge> queryGraph) {
		return Optional.of(queryGraph.vertexSet().stream()
				.filter(vertex -> !queryGraph.outgoingEdgesOf(vertex).isEmpty())
				.map(vertex -> queryGraph.outgoingEdgesOf(vertex).stream()
						.map(LabeledEdge::getPredicate)
						.distinct()
						.sorted()
						.map(Object::toString)
						.collect(Collectors.joining(",", "\"", "\"")))
				.collect(Collectors.joining(",", "[", "]")));
	}
}
