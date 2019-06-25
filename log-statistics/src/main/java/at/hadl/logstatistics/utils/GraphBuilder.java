package at.hadl.logstatistics.utils;

import at.hadl.logstatistics.LabeledEdge;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.apache.jena.sparql.syntax.ElementWalker;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class GraphBuilder {
	public static Optional<DefaultDirectedGraph<String, LabeledEdge>> constructGraphFromQuery(Query query, PredicateMap predicateMap) {
		List<Triple> triples = new ArrayList<>();
		if (query.getQueryPattern() == null) {
			return Optional.empty();
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

		triples.forEach(triple -> queryGraph.addEdge(triple.getSubject().toString(), triple.getObject().toString(), new LabeledEdge(predicateMap.getIntForPredicate(triple.getPredicate().toString()))));

		return Optional.of(queryGraph);
	}
}
