package at.hadl.logstatistics.analysis;

import at.hadl.logstatistics.utils.GraphBuilder;
import at.hadl.logstatistics.utils.PredicateMap;
import at.hadl.logstatistics.utils.QueryParser;
import at.hadl.logstatistics.utils.preprocessing.NoopPreprocessor;
import at.hadl.logstatistics.utils.preprocessing.Preprocessor;
import org.apache.jena.query.Query;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StarShapeSizeCounter {
	private Iterator<List<String>> logBatches;
	private Path outFile;
	private Preprocessor preprocessor = new NoopPreprocessor();

	public StarShapeSizeCounter(Iterator<List<String>> logBatches, Path outFile) {
		this.logBatches = logBatches;
		this.outFile = outFile;
	}

	public void startAnalysis() throws IOException {
		var starShapeFrequencies = new ArrayList<Map.Entry<Integer, Long>>();
		var predicateMap = new PredicateMap();
		AtomicLong validQueries = new AtomicLong(0);

		while (logBatches.hasNext()) {
			var batch = logBatches.next();

			var validQueryLines = batch.stream()
					.parallel()
					.flatMap(line -> preprocessor.extractQueryString(line).stream())
					.map(preprocessor::preprocessQueryString)
					.flatMap(queryString -> QueryParser.parseQuery(queryString).stream())
					.collect(Collectors.toList());
			validQueries.getAndAdd(validQueryLines.size());

			starShapeFrequencies.addAll(validQueryLines
					.stream()
					.parallel()
					.flatMap(query -> extractStarShapeSizes(query, predicateMap))
					.collect(Collectors.groupingByConcurrent(Function.identity(), Collectors.counting()))
					.entrySet());
		}

		var totalFrequencies = starShapeFrequencies.stream()
				.collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.summingLong(Map.Entry::getValue)))
				.entrySet()
				.stream()
				.sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
				.collect(Collectors.toList());

		try (var fileWriter = new FileWriter(outFile.toFile())) {
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

	private Stream<Integer> extractStarShapeSizes(Query query, PredicateMap predicateMap) {
		return new GraphBuilder().constructGraphFromQuery(query, predicateMap, null, null, null).stream()
				.flatMap(queryGraph -> queryGraph.vertexSet().stream()
						.map(vertex -> queryGraph.outgoingEdgesOf(vertex).size()));
	}
}
