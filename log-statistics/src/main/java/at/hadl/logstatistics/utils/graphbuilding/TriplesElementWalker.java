package at.hadl.logstatistics.utils.graphbuilding;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.walker.WalkerVisitor;
import org.apache.jena.sparql.expr.E_Exists;
import org.apache.jena.sparql.expr.E_NotExists;
import org.apache.jena.sparql.expr.ExprFunctionOp;
import org.apache.jena.sparql.expr.ExprVisitorBase;
import org.apache.jena.sparql.syntax.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class TriplesElementWalker {
	private TriplesPathWalker triplesPathWalker;
	private Set<QueryFeature> encounteredFeatures = new HashSet<>();
	private boolean containsUnsupportedFeature = false;

	TriplesElementWalker(TriplesPathWalker triplesPathWalker) {
		this.triplesPathWalker = triplesPathWalker;
	}

	List<List<Triple>> walk(Element el, List<List<Triple>> tripleCollections) {
		if (el instanceof ElementGroup) {
			return walk((ElementGroup) el, tripleCollections);
		} else if (el instanceof ElementOptional) {
			return walk((ElementOptional) el, tripleCollections);
		} else if (el instanceof ElementPathBlock) {
			return walk((ElementPathBlock) el, tripleCollections);
		} else if (el instanceof ElementUnion) {
			return walk((ElementUnion) el, tripleCollections);
		} else if (el instanceof ElementFilter) {
			return walk((ElementFilter) el, tripleCollections);
		} else if (el instanceof ElementMinus) {
			return walk((ElementMinus) el, tripleCollections);
		} else if (el instanceof ElementSubQuery) {
			return walk((ElementSubQuery) el, tripleCollections);
		} else if (el instanceof ElementDataset) {
			return walk((ElementDataset) el, tripleCollections);
		} else if (el instanceof ElementService) {
			return walk((ElementService) el, tripleCollections);
		} else if (el instanceof ElementNamedGraph) {
			return walk((ElementNamedGraph) el, tripleCollections);
		} else {
			System.out.println("Unsupported element type encountered!");
			System.out.println(el);
			containsUnsupportedFeature = true;
			return tripleCollections;
		}
	}


	private List<List<Triple>> walk(ElementGroup el, List<List<Triple>> tripleCollections) {
		encounteredFeatures.add(QueryFeature.GROUP);
		List<List<Triple>> resultTripleCollections = tripleCollections;
		for (Element element : el.getElements()) {
			resultTripleCollections = walk(element, resultTripleCollections);
		}
		return resultTripleCollections;
	}

	private List<List<Triple>> walk(ElementOptional el, List<List<Triple>> tripleCollections) {
		encounteredFeatures.add(QueryFeature.OPTIONAL);
		List<List<Triple>> modifiedTripleCollections = tripleCollections.stream()
				.map(ArrayList::new)
				.collect(Collectors.toList());

		modifiedTripleCollections = walk(el.getOptionalElement(), modifiedTripleCollections);

		return Stream.concat(tripleCollections.stream(), modifiedTripleCollections.stream()).collect(Collectors.toList());
	}

	private List<List<Triple>> walk(ElementUnion el, List<List<Triple>> tripleCollections) {
		encounteredFeatures.add(QueryFeature.UNION);
		return el.getElements().stream().flatMap(unionElement -> {
			List<List<Triple>> copiedTripleCollections = tripleCollections.stream()
					.map(ArrayList::new)
					.collect(Collectors.toList());

			return walk(unionElement, copiedTripleCollections).stream();
		}).collect(Collectors.toList());
	}

	private List<List<Triple>> walk(ElementFilter el, List<List<Triple>> tripleCollections) {
		encounteredFeatures.add(QueryFeature.FILTER);
		new WalkerVisitor(null, new ExprVisitorBase() {
			@Override
			public void visit(ExprFunctionOp expr) {
				if (expr instanceof E_Exists) {
					encounteredFeatures.add(QueryFeature.FILTER_EXISTS);
				} else if (expr instanceof E_NotExists) {
					encounteredFeatures.add(QueryFeature.FILTER_NOT_EXISTS);
				}

				List<List<Triple>> exprTripleCollections = new ArrayList<>();
				exprTripleCollections.add(new ArrayList<>());

				tripleCollections.addAll(walk(expr.getElement(), exprTripleCollections));
			}
		}, null, null).walk(el.getExpr());

		return tripleCollections;
	}

	private List<List<Triple>> walk(ElementMinus el, List<List<Triple>> tripleCollections) {
		encounteredFeatures.add(QueryFeature.MINUS);
		List<List<Triple>> minusTripleCollections = new ArrayList<>();
		minusTripleCollections.add(new ArrayList<>());

		tripleCollections.addAll(walk(el.getMinusElement(), minusTripleCollections));

		return tripleCollections;
	}

	private List<List<Triple>> walk(ElementPathBlock el, List<List<Triple>> tripleCollections) {
		for (var triplePath : el.getPattern().getList()) {
			if (triplePath.isTriple()) {
				final Triple triple = triplePath.asTriple();
				if (triple.getPredicate().isURI()) {
					tripleCollections.forEach(collection -> collection.add(triple));
				} else {
					encounteredFeatures.add(QueryFeature.VARIABLE_PREDICATE);
					containsUnsupportedFeature = true;
				}
			} else {
				encounteredFeatures.add(QueryFeature.PROPERTY_PATH);
				tripleCollections = triplesPathWalker.walk(triplePath.getPath(), tripleCollections, triplePath.getSubject(), triplePath.getObject());
			}

		}

		return tripleCollections;
	}

	private List<List<Triple>> walk(ElementDataset el, List<List<Triple>> tripleCollections) {
		encounteredFeatures.add(QueryFeature.DATASET);
		return walk(el.getElement(), tripleCollections);
	}

	private List<List<Triple>> walk(ElementService el, List<List<Triple>> tripleCollections) {
		encounteredFeatures.add(QueryFeature.SERVICE);
		return walk(el.getElement(), tripleCollections);
	}

	private List<List<Triple>> walk(ElementNamedGraph el, List<List<Triple>> tripleCollections) {
		encounteredFeatures.add(QueryFeature.NAMED_GRAPH);
		return walk(el.getElement(), tripleCollections);
	}

	private List<List<Triple>> walk(ElementSubQuery el, List<List<Triple>> tripleCollections) {
		encounteredFeatures.add(QueryFeature.SUB_QUERY);
		containsUnsupportedFeature = true;

		return tripleCollections;
	}

	Set<QueryFeature> getEncounteredQueryFeatures() {
		return encounteredFeatures;
	}

	Set<PathFeature> getEncounteredPathFeatures() {
		return triplesPathWalker.getEncounteredFeatures();
	}

	boolean containsUnsupportedFeature() {
		return containsUnsupportedFeature || triplesPathWalker.containsUnsupportedFeature();
	}
}
