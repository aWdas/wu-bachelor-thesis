package at.hadl.logstatistics.utils;

import at.hadl.logstatistics.LabeledEdge;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GraphBuilder {
	private TripleCollectingElementWalker tripleCollectingElementWalker = new TripleCollectingElementWalker();

	public List<DefaultDirectedGraph<String, LabeledEdge>> constructGraphsFromQuery(Query query, final PredicateMap predicateMap) {
		if (query.getQueryPattern() == null) {
			return Collections.emptyList();
		}

		List<List<Triple>> tripleLists = new ArrayList<>();
		tripleLists.add(new ArrayList<>());
		try {
			tripleLists = tripleCollectingElementWalker.walk(query.getQueryPattern(), tripleLists);
		} catch (RuntimeException e) {
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

	void setTripleCollectingElementWalker(TripleCollectingElementWalker tripleCollectingElementWalker) {
		this.tripleCollectingElementWalker = tripleCollectingElementWalker;
	}
}
