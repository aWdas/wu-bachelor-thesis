package at.hadl.logstatistics.utils;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.walker.WalkerVisitor;
import org.apache.jena.sparql.expr.ExprFunctionOp;
import org.apache.jena.sparql.expr.ExprVisitorBase;
import org.apache.jena.sparql.syntax.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TripleCollectingElementWalker {

	private TripleCollectingPathWalker tripleCollectingPathWalker = new TripleCollectingPathWalker();
	private List<String> encounteredMetaInformations = new ArrayList<>();

	public List<List<Triple>> walk(Element el, List<List<Triple>> tripleCollections) {
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
			System.out.println(el);
			return tripleCollections;
			// throw new RuntimeException("Unknown element type!");
		}
	}


	public List<List<Triple>> walk(ElementGroup el, List<List<Triple>> tripleCollections) {
		List<List<Triple>> resultTripleCollections = tripleCollections;
		for (Element element : el.getElements()) {
			resultTripleCollections = walk(element, resultTripleCollections);
		}
		return resultTripleCollections;
	}

	public List<List<Triple>> walk(ElementOptional el, List<List<Triple>> tripleCollections) {
		List<List<Triple>> modifiedTripleCollections = tripleCollections.stream()
				.map(ArrayList::new)
				.collect(Collectors.toList());

		modifiedTripleCollections = walk(el.getOptionalElement(), modifiedTripleCollections);

		return Stream.concat(tripleCollections.stream(), modifiedTripleCollections.stream()).collect(Collectors.toList());
	}

	public List<List<Triple>> walk(ElementUnion el, List<List<Triple>> tripleCollections) {
		return el.getElements().stream().flatMap(unionElement -> {
			List<List<Triple>> copiedTripleCollections = tripleCollections.stream()
					.map(ArrayList::new)
					.collect(Collectors.toList());

			return walk(unionElement, copiedTripleCollections).stream();
		}).collect(Collectors.toList());
	}

	public List<List<Triple>> walk(ElementFilter el, List<List<Triple>> tripleCollections) {
		new WalkerVisitor(null, new ExprVisitorBase() {
			@Override
			public void visit(ExprFunctionOp expr) {
				List<List<Triple>> exprTripleCollections = new ArrayList<>();
				exprTripleCollections.add(new ArrayList<>());

				tripleCollections.addAll(walk(expr.getElement(), exprTripleCollections));
			}
		}, null, null).walk(el.getExpr());

		return tripleCollections;
	}

	public List<List<Triple>> walk(ElementMinus el, List<List<Triple>> tripleCollections) {
		List<List<Triple>> minusTripleCollections = new ArrayList<>();
		minusTripleCollections.add(new ArrayList<>());

		tripleCollections.addAll(walk(el.getMinusElement(), minusTripleCollections));

		return tripleCollections;
	}

	public List<List<Triple>> walk(ElementPathBlock el, List<List<Triple>> tripleCollections) {
		for (var triplePath : el.getPattern().getList()) {
			if (triplePath.isTriple()) {
				final Triple triple = triplePath.asTriple();
				if (triple.getPredicate().isURI()) {
					tripleCollections.forEach(collection -> collection.add(triple));
				} else {
					throw new RuntimeException("Query contains variable predicates!");
				}

			} else {
				tripleCollections = tripleCollectingPathWalker.walk(triplePath.getPath(), tripleCollections, triplePath.getSubject(), triplePath.getObject());
			}

		}

		return tripleCollections;
	}

	public List<List<Triple>> walk(ElementDataset el, List<List<Triple>> tripleCollections) {
		System.out.println(el);
		return walk(el.getElement(), tripleCollections);
	}

	public List<List<Triple>> walk(ElementService el, List<List<Triple>> tripleCollections) {
		return walk(el.getElement(), tripleCollections);
	}

	public List<List<Triple>> walk(ElementNamedGraph el, List<List<Triple>> tripleCollections) {
		return walk(el.getElement(), tripleCollections);
	}

	public List<List<Triple>> walk(ElementSubQuery el, List<List<Triple>> tripleCollections) {
		throw new RuntimeException("Query contains subqueries!");
	}

	public void setTripleCollectingPathWalker(TripleCollectingPathWalker tripleCollectingPathWalker) {
		this.tripleCollectingPathWalker = tripleCollectingPathWalker;
	}
}
