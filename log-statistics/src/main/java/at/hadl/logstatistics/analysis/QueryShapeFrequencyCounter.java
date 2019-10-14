package at.hadl.logstatistics.analysis;

import at.hadl.logstatistics.utils.PredicateMap;
import at.hadl.logstatistics.utils.QueryParser;
import at.hadl.logstatistics.utils.graphbuilding.GraphBuilder;
import at.hadl.logstatistics.utils.graphbuilding.TriplesElementWalkerFactory;
import at.hadl.logstatistics.utils.graphbuilding.UUIDGenerator;
import at.hadl.logstatistics.utils.preprocessing.NoopPreprocessor;
import at.hadl.logstatistics.utils.preprocessing.Preprocessor;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import static at.hadl.logstatistics.utils.RequiredPartitionsExtractor.extractStarShapes;

/*
This class counts the shapes of the query graphs of a series of log lines.
It uses a "Preprocessor" to extract the query string form each log line.
Apache Jena ARQ's "QueryParser" transforms this query string into a "Query".
A "GraphBuilder" is used to transform this Apache Jena ARQ "Query" into a collection of "Query Graphs".
Finally, a "RequiredPartitionsExtractor" is used to get the required partitions to cover these query graphs.

The results of the analysis are written to two TSV files, one for the query shape counts and the other for collected meta-information.
*/
public class QueryShapeFrequencyCounter {
    private Iterator<List<String>> logBatches;
    private String outFile;
    private PredicateMap predicateMap;
    private ConcurrentHashMap<String, LongAdder> metaInformationCounters;
    private Preprocessor preprocessor = new NoopPreprocessor();
    private GraphBuilder graphBuilder = new GraphBuilder(new TriplesElementWalkerFactory(new UUIDGenerator()));

    public QueryShapeFrequencyCounter(Iterator<List<String>> logBatches, String outFile) {
        this.logBatches = logBatches;
        this.outFile = outFile;
        this.predicateMap = new PredicateMap();
        this.metaInformationCounters = new ConcurrentHashMap<>();
    }

    public QueryShapeFrequencyCounter withPreprocessor(Preprocessor preprocessor) {
        this.preprocessor = preprocessor;
        return this;
    }

    public QueryShapeFrequencyCounter withPredicateMap(PredicateMap predicateMap) {
        this.predicateMap = predicateMap;
        return this;
    }

    public void startAnalysis() throws IOException {
        ZonedDateTime start = ZonedDateTime.now();
        final ConcurrentHashMap<String, LongAdder> totalFrequencies = new ConcurrentHashMap<>();

        while (logBatches.hasNext()) {
            var batch = logBatches.next();
            Collections.shuffle(batch);

            batch.parallelStream()
                    .peek(line -> metaInformationCounters.computeIfAbsent("TOTAL_LINES", k -> new LongAdder()).increment())
                    .flatMap(line -> preprocessor.extractQueryString(line).stream())
                    .peek(line -> metaInformationCounters.computeIfAbsent("TOTAL_QUERIES", k -> new LongAdder()).increment())
                    .map(preprocessor::preprocessQueryString)
                    .flatMap(queryString -> QueryParser.parseQuery(queryString).stream())
                    .peek(query -> metaInformationCounters.computeIfAbsent("VALID_QUERIES", k -> new LongAdder()).increment())
                    .map(queryGraph -> graphBuilder.constructGraphsFromQuery(queryGraph, predicateMap))
                    .map(graphBuildingResult -> {
                        graphBuildingResult.getEncounteredFeatures()
                                .forEach(featureKey -> metaInformationCounters.computeIfAbsent(featureKey, k -> new LongAdder()).increment());

                        return extractStarShapes(graphBuildingResult.getConstructedGraphs());
                    })
                    .forEach(queryShapeOptional -> queryShapeOptional.ifPresent(queryShape -> totalFrequencies.computeIfAbsent(queryShape, k -> new LongAdder()).increment()));

            Duration executionDuration = Duration.between(start, ZonedDateTime.now());
            System.out.println("Batch complete!");
            System.out.println("Total duration: " + executionDuration.toString());
            System.out.println();
        }

        var sortedTotalFrequencies = totalFrequencies.entrySet().stream()
                .map(e -> Map.entry(e.getKey(), e.getValue().sum()))
                .sorted(Comparator.comparing(Map.Entry::getValue, Comparator.reverseOrder()))
                .collect(Collectors.toList());

        System.out.println("PredicateMap size: " + predicateMap.size());

        writeResults(sortedTotalFrequencies);
    }

    private void writeResults(List<Map.Entry<String, Long>> sortedTotalFrequencies) throws IOException {
        try (var fileWriter = new FileWriter(outFile + "_meta.tsv")) {
            for (var entry : metaInformationCounters.entrySet()) {
                fileWriter.write(entry.getKey() + "\t" + entry.getValue().sum() + "\n");
            }
        }

        try (var fileWriter = new FileWriter(outFile + ".tsv")) {
            fileWriter.write("query_shape\tcount\n");
            sortedTotalFrequencies.forEach(entry -> {
                try {
                    fileWriter.write(entry.getKey() + "\t" + entry.getValue() + "\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }
}
