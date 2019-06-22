package at.hadl.logstatistics.analysis;

import at.hadl.logstatistics.utils.BatchLogIterator;
import at.hadl.logstatistics.utils.GraphBuilder;
import at.hadl.logstatistics.utils.Preprocessing;
import at.hadl.logstatistics.utils.QueryParser;
import org.apache.jena.query.Query;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StarShapeSizeCounter {
	private BatchLogIterator logBatches;
	private Path outFile;

	public StarShapeSizeCounter(BatchLogIterator logBatches, Path outFile) {
		this.logBatches = logBatches;
		this.outFile = outFile;
	}

	public void startAnalysis() throws IOException {
		var starShapeFrequencies = new ArrayList<Map.Entry<Integer, Long>>();

		while (logBatches.hasNext()) {
			var batch = logBatches.next();

			starShapeFrequencies.addAll(batch.stream()
					.parallel()
					.flatMap(line -> Preprocessing.extractQueryString(line).stream())
					.map(Preprocessing::preprocessVirtuosoQueryString)
					.flatMap(queryString -> QueryParser.parseQuery(queryString).stream())
					.flatMap(this::extractStarShapeSizes)
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
			totalFrequencies.forEach(entry -> {
				try {
					fileWriter.write(entry.getKey().toString() + "\t" + entry.getValue() + "\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
		}
	}

	private Stream<Integer> extractStarShapeSizes(Query query) {
		return GraphBuilder.constructGraphFromQuery(query)
				.map(queryGraph -> queryGraph.vertexSet().stream()
						.map(vertex -> queryGraph.outgoingEdgesOf(vertex).size()))
				.orElse(Stream.empty());
	}
}
