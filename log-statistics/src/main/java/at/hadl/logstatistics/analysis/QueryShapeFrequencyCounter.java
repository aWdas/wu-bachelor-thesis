package at.hadl.logstatistics.analysis;

import at.hadl.logstatistics.utils.PredicateMap;
import at.hadl.logstatistics.utils.QueryParser;
import at.hadl.logstatistics.utils.graphbuilding.GraphBuilder;
import at.hadl.logstatistics.utils.graphbuilding.LabeledEdge;
import at.hadl.logstatistics.utils.graphbuilding.TriplesElementWalkerFactory;
import at.hadl.logstatistics.utils.graphbuilding.UUIDGenerator;
import at.hadl.logstatistics.utils.preprocessing.NoopPreprocessor;
import at.hadl.logstatistics.utils.preprocessing.Preprocessor;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class QueryShapeFrequencyCounter {
	private Iterator<List<String>> logBatches;
	private Path outFile;
	private Path predicateMapOutPath;
	private PredicateMap predicateMap;
	private ConcurrentHashMap<String, Integer> metaInformationCounters;
	private Preprocessor preprocessor = new NoopPreprocessor();
	private GraphBuilder graphBuilder = new GraphBuilder(new TriplesElementWalkerFactory(new UUIDGenerator()));

	private BiFunction<String, Integer, Integer> incrementByOne = (key, count) -> (count == null) ? 1 : count + 1;

	public QueryShapeFrequencyCounter(Iterator<List<String>> logBatches, Path outFile, Path predicateMapOutPath) {
		this.logBatches = logBatches;
		this.outFile = outFile;
		this.predicateMapOutPath = predicateMapOutPath;
		this.predicateMap = new PredicateMap();
		this.metaInformationCounters = new ConcurrentHashMap<>();
	}

	public QueryShapeFrequencyCounter withPreprocessor(Preprocessor preprocessor) {
		this.preprocessor = preprocessor;
		return this;
	}

	public QueryShapeFrequencyCounter withPredicateMap(Path predicateMapPath) {
		this.predicateMap = PredicateMap.fromPath(predicateMapPath).orElseThrow();
		return this;
	}

	private static String extractStarShapes(List<DefaultDirectedGraph<String, LabeledEdge>> queryGraphs) {
		return queryGraphs.stream()
				.flatMap(queryGraph -> queryGraph.vertexSet().stream()
						.filter(vertex -> !queryGraph.outgoingEdgesOf(vertex).isEmpty())
						.map(vertex -> queryGraph.outgoingEdgesOf(vertex).stream()
								.map(LabeledEdge::getPredicate)
								.distinct()
								.sorted()
								.map(Object::toString)
								.collect(Collectors.joining(",", "\"", "\""))))
				.distinct()
				.collect(Collectors.joining(",", "[", "]"));
	}

	public void startAnalysis() throws IOException {
		ZonedDateTime start = ZonedDateTime.now();
		final ConcurrentHashMap<String, Integer> totalFrequencies = new ConcurrentHashMap<>();

		while (logBatches.hasNext()) {
			var batch = logBatches.next();
			Collections.shuffle(batch);

			batch.parallelStream()
					.peek(line -> metaInformationCounters.compute("TOTAL_LINES", incrementByOne))
					.flatMap(line -> preprocessor.extractQueryString(line).stream())
					.peek(line -> metaInformationCounters.compute("TOTAL_QUERIES", incrementByOne))
					.map(preprocessor::preprocessQueryString)
					.flatMap(queryString -> QueryParser.parseQuery(queryString).stream())
					.peek(query -> metaInformationCounters.compute("VALID_QUERIES", incrementByOne))
					.map(queryGraph -> graphBuilder.constructGraphsFromQuery(queryGraph, predicateMap))
					.map(graphBuildingResult -> {
						graphBuildingResult.getEncounteredFeatures()
								.forEach(featureKey -> metaInformationCounters.compute(featureKey, incrementByOne));

						return extractStarShapes(graphBuildingResult.getConstructedGraphs());
					})
					.forEach(queryShape -> totalFrequencies.compute(queryShape, incrementByOne));

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
			for (var entry : metaInformationCounters.entrySet()) {
				fileWriter.write(entry.getKey() + "\t" + entry.getValue() + "\n");
			}
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
}
