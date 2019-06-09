package at.hadl.logstatistics;

import com.google.common.collect.Sets;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryException;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.apache.jena.sparql.syntax.ElementWalker;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import javax.print.attribute.SetOfIntegerSyntax;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public class Application {
    public static void main(String[] args) throws IOException {
        //Query q = QueryFactory.create("SELECT (COUNT(?uri) AS ?count) WHERE { ?uri rdf:type dbpedia-owl:University . ?uri foaf:name ?name . }");
        //Pattern regex = Pattern.compile("sparql\\t(.*)");

//        final String defaultPrefixes = new BufferedReader(new InputStreamReader(ClassLoader.getSystemResourceAsStream("dbpedia-prefix-mappings.txt"))).lines()
//                .map(prefixLine -> {
//                    var prefixLineParts = prefixLine.split("\\t");
//                    return "PREFIX " + prefixLineParts[0] + ":<" + prefixLineParts[1] + ">";
//                })
//                .collect(Collectors.joining("\n"));

        Stream<String> logStream = new BufferedReader(new InputStreamReader(new GZIPInputStream(ClassLoader.getSystemResourceAsStream("access-logs/british-museum/sparql.log.1.gz")))).lines();
        var predicateGroupFrequencies = logStream
                .map(logLine -> {
                    //Matcher m = regex.matcher(logLine);

//                    if (m.find()) {
//                        String queryString = m.group(1);
//                        return Stream.of(queryString);
//                    } else {
//                        return Stream.empty();
//                    }
                    return logLine.split("\\t")[3];
                })
                .parallel()
                .flatMap(encodedString -> decodeURLEncodedString(encodedString).stream())
//                .map(queryString -> defaultPrefixes + "\n" + queryString)
                .flatMap(queryString -> parseQuery(queryString).stream())
                .flatMap(Application::extractStarShapePredicateCombinations)
                .collect(Collectors.groupingByConcurrent(predicateSet -> predicateSet, Collectors.counting()))
                .entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toList());

        predicateGroupFrequencies.forEach(System.out::println);
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
            return Optional.empty();
        }
    }

    private static Stream<Set<String>> extractStarShapePredicateCombinations(Query query) {
        List<Triple> triples = new ArrayList<>();
        if(query.getQueryPattern() == null) {
            System.out.println(query.toString());
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

                    IntStream.rangeClosed(1, outgoingPredicates.size()).forEach(size -> starCombinations.addAll(Sets.combinations(outgoingPredicates, size)));
                    return starCombinations.stream();
                });
    }
}
