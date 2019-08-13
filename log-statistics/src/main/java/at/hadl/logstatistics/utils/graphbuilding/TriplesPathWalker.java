package at.hadl.logstatistics.utils.graphbuilding;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Variable;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.path.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class TriplesPathWalker {
    private UUIDGenerator uuidGenerator;
    private Set<PathFeature> encounteredFeatures = new HashSet<>();
    private boolean containsUnsupportedFeature = false;

    TriplesPathWalker(UUIDGenerator uuidGenerator) {
        this.uuidGenerator = uuidGenerator;
    }

    List<List<Triple>> walk(Path path, Node start, Node end) {
        if (path instanceof P_Link) {
            return walk((P_Link) path, start, end);
        } else if (path instanceof P_NegPropSet) {
            return walk((P_NegPropSet) path, start, end);
        } else if (path instanceof P_Inverse) {
            return walk((P_Inverse) path, start, end);
        } else if (path instanceof P_ZeroOrOne) {
            return walk((P_ZeroOrOne) path, start, end);
        } else if (path instanceof P_ZeroOrMore1) {
            return walk((P_ZeroOrMore1) path, start, end);
        } else if (path instanceof P_OneOrMore1) {
            return walk((P_OneOrMore1) path, start, end);
        } else if (path instanceof P_Alt) {
            return walk((P_Alt) path, start, end);
        } else if (path instanceof P_Seq) {
            return walk((P_Seq) path, start, end);
        } else {
            System.out.println("Unknown path type encountered!");
            System.out.println(path);
            containsUnsupportedFeature = true;
            return Collections.emptyList();
        }
    }

    private List<List<Triple>> walk(P_Link path, Node start, Node end) {
        return Collections.singletonList(Collections.singletonList(new Triple(start, path.getNode(), end)));
    }

    private List<List<Triple>> walk(P_NegPropSet path, Node start, Node end) {
        encounteredFeatures.add(PathFeature.NEGATED_PROP_SET);
        containsUnsupportedFeature = true;

        return Collections.emptyList();
    }

    private List<List<Triple>> walk(P_Inverse path, Node start, Node end) {
        encounteredFeatures.add(PathFeature.INVERSE);

        return walk(path.getSubPath(), end, start);
    }

    private List<List<Triple>> walk(P_ZeroOrOne path, Node start, Node end) {
        encounteredFeatures.add(PathFeature.ZERO_OR_ONE);

        List<List<Triple>> appliedOnceCollections = walk(path.getSubPath(), start, end);

        List<List<Triple>> tripleCollections = new ArrayList<>(appliedOnceCollections.size() + 1);
        tripleCollections.add(Collections.emptyList());
        tripleCollections.addAll(appliedOnceCollections);

        return tripleCollections;
    }

    private List<List<Triple>> walk(P_ZeroOrMore1 path, Node start, Node end) {
        encounteredFeatures.add(PathFeature.ZERO_OR_MORE);

        List<List<Triple>> appliedOnceCollections = walk(path.getSubPath(), start, end);

        Node center = new Node_Variable(uuidGenerator.generateUUID());
        List<List<Triple>> appliedTwiceCollections = crossCombine(walk(path.getSubPath(), start, center), walk(path.getSubPath(), center, end));


        List<List<Triple>> tripleCollections = new ArrayList<>(appliedOnceCollections.size() + appliedTwiceCollections.size() + 1);
        tripleCollections.add(Collections.emptyList());
        tripleCollections.addAll(appliedOnceCollections);
        tripleCollections.addAll(appliedTwiceCollections);

        return tripleCollections;
    }

    private List<List<Triple>> walk(P_OneOrMore1 path, Node start, Node end) {
        encounteredFeatures.add(PathFeature.ONE_OR_MORE);

        List<List<Triple>> appliedOnceCollections = walk(path.getSubPath(), start, end);

        Node center = new Node_Variable(uuidGenerator.generateUUID());
        List<List<Triple>> appliedTwiceCollections = crossCombine(walk(path.getSubPath(), start, center), walk(path.getSubPath(), center, end));

        return Stream.concat(appliedOnceCollections.stream(), appliedTwiceCollections.stream()).collect(Collectors.toList());
    }

    private List<List<Triple>> walk(P_Alt path, Node start, Node end) {
        encounteredFeatures.add(PathFeature.ALT);

        List<List<Triple>> leftCollections = walk(path.getLeft(), start, end);
        List<List<Triple>> rightCollections = walk(path.getRight(), start, end);

        return Stream.concat(leftCollections.stream(), rightCollections.stream()).collect(Collectors.toList());
    }

    private List<List<Triple>> walk(P_Seq path, Node start, Node end) {
        encounteredFeatures.add(PathFeature.SEQ);

        List<List<Triple>> tripleCollections = new ArrayList<>();

        Node center = new Node_Variable(uuidGenerator.generateUUID());
        var leftCollectionsByEmpty = walk(path.getLeft(), start, center).stream()
                .collect(Collectors.partitioningBy((List<Triple> list) -> list.size() == 0));
        var rightCollectionsByEmpty = walk(path.getRight(), center, end).stream()
                .collect(Collectors.partitioningBy((List<Triple> list) -> list.size() == 0));

        var emptyLeftCollections = leftCollectionsByEmpty.get(true);
        var nonEmptyLeftCollections = leftCollectionsByEmpty.get(false);
        var emptyRightCollections = rightCollectionsByEmpty.get(true);
        var nonEmptyRightCollections = rightCollectionsByEmpty.get(false);

        tripleCollections.addAll(crossCombine(nonEmptyLeftCollections, nonEmptyRightCollections));

        if (emptyLeftCollections.size() > 0) {
            var correctedRightCollections = replaceNode(nonEmptyRightCollections, center, start);
            tripleCollections.addAll(crossCombine(emptyLeftCollections, correctedRightCollections));
        }

        if (emptyRightCollections.size() > 0) {
            var correctedLeftCollections = replaceNode(nonEmptyLeftCollections, center, end);
            tripleCollections.addAll(crossCombine(correctedLeftCollections, emptyRightCollections));
        }

        return tripleCollections;
    }

    private List<List<Triple>> replaceNode(List<List<Triple>> tripleCollections, Node from, Node to) {
        return tripleCollections.stream()
                .map(tripleCollection -> tripleCollection.stream()
                        .map(triple -> new Triple(
                                triple.getSubject().equals(from) ? to : triple.getSubject(),
                                triple.getPredicate(),
                                triple.getObject().equals(from) ? to : triple.getObject()))
                        .collect(Collectors.toList()))
                .collect(Collectors.toList());
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

    Set<PathFeature> getEncounteredFeatures() {
        return encounteredFeatures;
    }

    boolean containsUnsupportedFeature() {
        return containsUnsupportedFeature;
    }
}
