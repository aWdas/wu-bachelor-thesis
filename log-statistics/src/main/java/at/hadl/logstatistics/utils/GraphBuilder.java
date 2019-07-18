package at.hadl.logstatistics.utils;

import at.hadl.logstatistics.LabeledEdge;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.syntax.*;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

public class GraphBuilder {
	public static Optional<DefaultDirectedGraph<String, LabeledEdge>> constructGraphFromQuery(Query query, PredicateMap predicateMap, LongAdder variablePredicateCounter, LongAdder subQueriesCounter, LongAdder predicatePathsCounter) {
		List<Triple> triples = new ArrayList<>();
		AtomicBoolean hasVariablePredicates = new AtomicBoolean(false);
		AtomicBoolean hasSubqueries = new AtomicBoolean(false);
		AtomicBoolean hasPredicatePaths = new AtomicBoolean(false);
		if (query.getQueryPattern() == null) {
			return Optional.empty();
		}

		ElementWalker.walk(query.getQueryPattern(), new RecursiveElementVisitor(new ElementVisitorBase()) {
					@Override
					public void endElement(ElementPathBlock el) {
						el.getPattern().getList().forEach(triplePath -> {
							if (triplePath.isTriple()) {
								Triple triple = triplePath.asTriple();
								if (triple.getPredicate().isURI()) {
									triples.add(triple);
								} else {
									hasVariablePredicates.set(true);
								}
							} else {
								hasPredicatePaths.set(true);
							}
						});
					}

			public void endElement(ElementSubQuery el) {
				hasSubqueries.set(true);
			}
				}
		);

		if (hasVariablePredicates.get()) {
			if(variablePredicateCounter != null) {
				variablePredicateCounter.increment();
			}
		}

		if (hasSubqueries.get()) {
			if (subQueriesCounter != null) {
				subQueriesCounter.increment();
			}
		}

		if (hasPredicatePaths.get()) {
			System.out.println(query);
			if (predicatePathsCounter != null) {
				predicatePathsCounter.increment();
			}
		}

		if (hasPredicatePaths.get() || hasSubqueries.get() || hasVariablePredicates.get()) {
			return Optional.empty();
		}

		var queryGraph = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
		triples.stream()
				.flatMap(triple -> Stream.of(triple.getSubject().toString(), triple.getObject().toString()))
				.distinct()
				.forEach(queryGraph::addVertex);

		triples.forEach(triple -> queryGraph.addEdge(triple.getSubject().toString(), triple.getObject().toString(), new LabeledEdge(predicateMap.getIntForPredicate(triple.getPredicate().toString()))));

		return Optional.of(queryGraph);
	}
}
