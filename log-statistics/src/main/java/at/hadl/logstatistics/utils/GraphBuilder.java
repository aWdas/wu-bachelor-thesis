package at.hadl.logstatistics.utils;

import at.hadl.logstatistics.LabeledEdge;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GraphBuilder {
	public static List<DefaultDirectedGraph<String, LabeledEdge>> constructGraphFromQuery(Query query, final PredicateMap predicateMap, LongAdder variablePredicateCounter, LongAdder subQueriesCounter, LongAdder predicatePathsCounter) {
		AtomicBoolean hasVariablePredicates = new AtomicBoolean(false);
		AtomicBoolean hasSubqueries = new AtomicBoolean(false);
		AtomicBoolean hasPredicatePaths = new AtomicBoolean(false);

		if (query.getQueryPattern() == null) {
			return Collections.emptyList();
		}

		List<List<Triple>> tripleLists = new ArrayList<>();
		tripleLists.add(new ArrayList<>());
		tripleLists = TripleCollectingElementWalker.walk(query.getQueryPattern(), tripleLists);

		if (hasVariablePredicates.get()) {
			if (variablePredicateCounter != null) {
				variablePredicateCounter.increment();
			}
		}

		if (hasSubqueries.get()) {
			if (subQueriesCounter != null) {
				subQueriesCounter.increment();
			}
		}

		if (hasPredicatePaths.get()) {
			if (predicatePathsCounter != null) {
				predicatePathsCounter.increment();
			}
		}

		if (hasPredicatePaths.get() || hasSubqueries.get() || hasVariablePredicates.get()) {
			return Collections.emptyList();
		}

		return tripleLists.stream().map(triples -> {
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
	}
}
