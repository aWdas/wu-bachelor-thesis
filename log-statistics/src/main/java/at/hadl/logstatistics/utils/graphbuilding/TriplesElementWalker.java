package at.hadl.logstatistics.utils.graphbuilding;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.walker.WalkerVisitor;
import org.apache.jena.sparql.expr.E_Exists;
import org.apache.jena.sparql.expr.E_NotExists;
import org.apache.jena.sparql.expr.ExprFunctionOp;
import org.apache.jena.sparql.expr.ExprVisitorBase;
import org.apache.jena.sparql.syntax.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class TriplesElementWalker {
    private TriplesPathWalker triplesPathWalker;
    private Set<QueryFeature> encounteredFeatures = new HashSet<>();
    private boolean containsUnsupportedFeature = false;

    TriplesElementWalker(TriplesPathWalker triplesPathWalker) {
        this.triplesPathWalker = triplesPathWalker;
    }

    TripleCollectionResult walk(Element element) {
        if (element instanceof ElementGroup) {
            return walk((ElementGroup) element);
        } else if (element instanceof ElementSubQuery) {
            encounteredFeatures.add(QueryFeature.SUB_QUERY);
            containsUnsupportedFeature = true;
            return new TripleCollectionResult(Collections.emptyList(), Collections.emptyList());
        } else {
            System.out.println("Other block encountered");
            return new TripleCollectionResult(Collections.emptyList(), Collections.emptyList());
        }
    }

    TripleCollectionResult walk(ElementGroup elementGroup) {
        List<List<Triple>> mainQueryGraphs = new ArrayList<>();
        mainQueryGraphs.add(new ArrayList<>());
        List<List<Triple>> additionalQueryGraphs = new ArrayList<>();
        List<List<Triple>> optionals = new ArrayList<>();

//        var nonGroupElementsStream = elementGroup.getElements().stream()
//                .filter(el -> !(el instanceof ElementGroup));
//        var flattenedGroupElementChildrenStream = elementGroup.getElements().stream()
//                .filter(el -> (el instanceof ElementGroup))
//                .flatMap(el -> ((ElementGroup) el).getElements().stream());
//        List<Element> elements = Stream.concat(nonGroupElementsStream, flattenedGroupElementChildrenStream)
//                .collect(Collectors.toList());

        for (Element el : elementGroup.getElements()) {
            if (el instanceof ElementGroup) {
                var intermediateResult = walk(el);
                additionalQueryGraphs.addAll(intermediateResult.getAdditionalQueryGraphs());
                mainQueryGraphs = crossCombine(mainQueryGraphs, intermediateResult.getMainQueryGraphs());
            } else if (el instanceof ElementOptional) {
                encounteredFeatures.add(QueryFeature.OPTIONAL);
                var elementOptional = (ElementOptional) el;
                var intermediateResult = walk(elementOptional.getOptionalElement());

                additionalQueryGraphs.addAll(intermediateResult.getAdditionalQueryGraphs());
                optionals.addAll(intermediateResult.getMainQueryGraphs());

            } else if (el instanceof ElementPathBlock) {
                var tripleCollections = walk((ElementPathBlock) el);
                mainQueryGraphs = crossCombine(mainQueryGraphs, tripleCollections);

            } else if (el instanceof ElementUnion) {
                encounteredFeatures.add(QueryFeature.UNION);
                List<List<Triple>> intermediateMainGraphs = new ArrayList<>();
                for (Element unionElement : ((ElementUnion) el).getElements()) {
                    var intermediateResult = walk(unionElement);
                    intermediateMainGraphs.addAll(intermediateResult.getMainQueryGraphs());
                    additionalQueryGraphs.addAll(intermediateResult.getAdditionalQueryGraphs());
                }
                mainQueryGraphs = crossCombine(mainQueryGraphs, intermediateMainGraphs);

            } else if (el instanceof ElementFilter) {

                encounteredFeatures.add(QueryFeature.FILTER);
                new WalkerVisitor(null, new ExprVisitorBase() {
                    @Override
                    public void visit(ExprFunctionOp expr) {
                        if (expr instanceof E_Exists) {
                            encounteredFeatures.add(QueryFeature.FILTER_EXISTS);
                        } else if (expr instanceof E_NotExists) {
                            encounteredFeatures.add(QueryFeature.FILTER_NOT_EXISTS);
                        }

                        var intermediateResult = walk(expr.getElement());
                        additionalQueryGraphs.addAll(intermediateResult.getAdditionalQueryGraphs());
                        additionalQueryGraphs.addAll(intermediateResult.getMainQueryGraphs());
                    }
                }, null, null).walk(((ElementFilter) el).getExpr());

            } else if (el instanceof ElementMinus) {
                encounteredFeatures.add(QueryFeature.MINUS);
                var intermediateResult = walk(((ElementMinus) el).getMinusElement());
                additionalQueryGraphs.addAll(intermediateResult.getAdditionalQueryGraphs());
                additionalQueryGraphs.addAll(intermediateResult.getMainQueryGraphs());

            } else if (el instanceof ElementSubQuery) {
                encounteredFeatures.add(QueryFeature.SUB_QUERY);
                containsUnsupportedFeature = true;

            } else if (el instanceof ElementDataset) {
                encounteredFeatures.add(QueryFeature.DATASET);

            } else if (el instanceof ElementService) {
                encounteredFeatures.add(QueryFeature.SERVICE);

            } else if (el instanceof ElementNamedGraph) {
                encounteredFeatures.add(QueryFeature.NAMED_GRAPH);

            } else if (!(el instanceof ElementBind || el instanceof ElementData || el instanceof ElementAssign)) {
                System.out.println("Unsupported element type encountered!");
                System.out.println(el.getClass());
                System.out.println(elementGroup);
                containsUnsupportedFeature = true;
            }
        }

        if (optionals.size() > 0) {
            mainQueryGraphs = Stream.concat(mainQueryGraphs.stream(), crossCombine(mainQueryGraphs, optionals).stream())
                    .collect(Collectors.toList());
        }

        return TripleCollectionResult.builder()
                .mainQueryGraphs(mainQueryGraphs)
                .additionalQueryGraphs(additionalQueryGraphs)
                .build();
    }

    private List<List<Triple>> walk(ElementPathBlock el) {
        List<List<Triple>> tripleCollections = new ArrayList<>();
        tripleCollections.add(new ArrayList<>());

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
                tripleCollections = crossCombine(tripleCollections, triplesPathWalker.walk(triplePath.getPath(), triplePath.getSubject(), triplePath.getObject()));
            }

        }

        return tripleCollections;
    }

    private List<List<Triple>> crossCombine(List<List<Triple>> collection1, List<List<Triple>> collection2) {
        if (collection1.size() == 0) {
            return collection2;
        } else if (collection2.size() == 0) {
            return collection1;
        }

        return collection1.stream()
                .flatMap(sourceCollection -> collection2.stream()
                        .map(addition -> Stream.concat(sourceCollection.stream(), addition.stream()).collect(Collectors.toList())))
                .collect(Collectors.toList());
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
