package at.hadl.logstatistics.utils.graphbuilding;

import at.hadl.logstatistics.utils.PredicateMap;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GraphBuilder {
    private TriplesElementWalkerFactory triplesElementWalkerFactory;

    public GraphBuilder(TriplesElementWalkerFactory triplesElementWalkerFactory) {
        this.triplesElementWalkerFactory = triplesElementWalkerFactory;
    }

    public GraphBuildingResult constructGraphsFromQuery(Query query, final PredicateMap predicateMap) {
        if (query.getQueryPattern() == null) {
            return new GraphBuildingResult(Collections.emptyList(), Collections.emptySet());
        }

        TriplesElementWalker triplesElementWalker = triplesElementWalkerFactory.createTripleElementWalker();
        List<List<Triple>> tripleLists = new ArrayList<>();
        tripleLists.add(new ArrayList<>());

        tripleLists = triplesElementWalker.walk(query.getQueryPattern(), tripleLists);

        var encounteredFeatures = Stream.concat(
                triplesElementWalker.getEncounteredQueryFeatures().stream(),
                triplesElementWalker.getEncounteredPathFeatures().stream())
                .map(Enum::name)
                .collect(Collectors.toSet());


        if (triplesElementWalker.containsUnsupportedFeature()) {
            return new GraphBuildingResult(Collections.emptyList(), encounteredFeatures);
        } else {
            var queryGraphs = tripleLists.stream()
                    .map(triples -> {
                        final DefaultDirectedGraph<String, LabeledEdge> queryGraph = new DefaultDirectedGraph<>(LabeledEdge.class);
                        triples.stream()
                                .flatMap(triple -> Stream.of(triple.getSubject().toString(), triple.getObject().toString()))
                                .distinct()
                                .forEach(queryGraph::addVertex);

                        triples.forEach(triple -> queryGraph.addEdge(
                                triple.getSubject().toString(),
                                triple.getObject().toString(),
                                new LabeledEdge(predicateMap.getIntForPredicate(triple.getPredicate().toString()))
                        ));

                        return queryGraph;
                    }).collect(Collectors.toList());

            return new GraphBuildingResult(queryGraphs, encounteredFeatures);
        }
    }
}
