package at.hadl.logstatistics;

import com.google.common.collect.Sets;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryException;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.apache.jena.sparql.syntax.ElementWalker;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.io.*;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Application {
    private static final int BATCH_SIZE = 5000;

    public static void main(String[] args) throws IOException {
        Pattern regex = Pattern.compile("query=(.*?)(&| HTTP)");
        String defaultPrefixes = getDefaultPrefixes();
        ZonedDateTime start = ZonedDateTime.now();
        List<Map.Entry<Set<String>, Long>> allFrequencies = new ArrayList<>();

        try (var files = Files.walk(Paths.get(System.getProperty("user.home"),"Desktop", "access-logs", "usewod-2016"))) {
            files
                    .filter(Files::isRegularFile)
                    .forEach(path -> {
                        System.out.println("--------------------------------------------------------------------");
                        System.out.println("Working on file: " + path.toString());
                        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new BZip2CompressorInputStream(new FileInputStream(path.toFile()), true)))) {
                            List<String> batch;

                            while((batch = getNextBatch(reader)).size() > 0) {
                                var predicateGroupFrequencies = new ArrayList<>(batch.stream()
                                        .parallel()
                                        .flatMap(logLine -> {
                                            Matcher m = regex.matcher(logLine);

                                            if (m.find()) {
                                                String queryString = m.group(1);
                                                return Stream.of(queryString);
                                            } else {
                                                return Stream.empty();
                                            }
                                        })
                                        .flatMap(encodedString -> decodeURLEncodedString(encodedString).stream())
                                        .map(queryString -> defaultPrefixes + "\n" + queryString)
                                        .flatMap(queryString -> parseQuery(queryString).stream())
                                        .flatMap(Application::extractStarShapePredicateCombinations)
                                        .collect(Collectors.groupingByConcurrent(predicateSet -> predicateSet, Collectors.counting()))
                                        .entrySet());

                                allFrequencies.addAll(predicateGroupFrequencies);

                                System.out.println("Batch processed!");
                                Duration executionDuration = Duration.between(start, ZonedDateTime.now());
                                System.out.println(executionDuration.toString());
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
        }

        var totalFrequencies = allFrequencies.stream()
                .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.summingLong(Map.Entry::getValue)))
                .entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toList());

        try(var fileWriter = new FileWriter("stats2.txt")) {
            totalFrequencies.forEach(entry -> {
                try {
                    fileWriter.write(entry.getKey().toString() + "\t" + entry.getValue() + "\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private static String getDefaultPrefixes() throws IOException {
        String defaultPrefixes;

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(ClassLoader.getSystemResourceAsStream("prefix-mappings-reduced.txt")))) {
            defaultPrefixes = reader.lines()
                    .map(prefixLine -> {
                        var prefixLineParts = prefixLine.split("=");
                        return "PREFIX " + prefixLineParts[0] + ":<" + prefixLineParts[1] + ">";
                    })
                    .collect(Collectors.joining("\n"));
        }

        return defaultPrefixes;
    }

    private static List<String> getNextBatch(BufferedReader reader) throws IOException {
        List<String> lines = new ArrayList<>();

        for(int i = 0; i <= BATCH_SIZE; i++) {
            var line = reader.readLine();
            if(line == null) {
                break;
            } else {
                lines.add(line);
            }
        }

        return lines;
    }

    private static Optional<String> decodeURLEncodedString(String encodedString) {
        try {
            return Optional.of(URLDecoder.decode(encodedString, StandardCharsets.UTF_8));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static Optional<Query> parseQuery(String queryString) {
        try {
            return Optional.of(QueryFactory.create(queryString, Syntax.defaultSyntax));
        } catch (QueryException e) {
            if(e.getMessage().contains("Unresolved prefixed name")) {
                System.out.println(e);
            }
            return Optional.empty();
        }
    }

    private static Stream<Set<String>> extractStarShapePredicateCombinations(Query query) {
        List<Triple> triples = new ArrayList<>();
        if (query.getQueryPattern() == null) {
            return Stream.empty();
        }

        ElementWalker.walk(query.getQueryPattern(), new ElementVisitorBase() {
            @Override
            public void visit(ElementPathBlock el) {
                el.getPattern().getList().forEach(triplePath -> {
                    Triple triple = triplePath.asTriple();
                    if (triple != null && triple.getPredicate().isURI()) {
                        triples.add(triple);
                    }
                });
            }
        });

        var queryGraph = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        triples.stream()
                .flatMap(triple -> Stream.of(triple.getSubject().toString(), triple.getObject().toString()))
                .distinct()
                .forEach(queryGraph::addVertex);

        triples.forEach(triple -> queryGraph.addEdge(triple.getSubject().toString(), triple.getObject().toString(), new LabeledEdge(triple.getPredicate().toString())));

        return queryGraph.vertexSet().stream()
                .flatMap(vertex -> {
                    var outgoingPredicates = queryGraph.outgoingEdgesOf(vertex).stream().map(LabeledEdge::getPredicate).collect(Collectors.toSet());
                    Set<Set<String>> starCombinations = new HashSet<>();

                    IntStream.rangeClosed(1, Math.min(4, outgoingPredicates.size())).forEach(size -> starCombinations.addAll(Sets.combinations(outgoingPredicates, size)));
                    return starCombinations.stream();
                });
    }
}
