package at.hadl.logstatistics.utils.graphbuilding;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Variable;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.path.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class TriplesPathWalker {
	private UUIDGenerator uuidGenerator;
	private Set<PathFeature> encounteredFeatures = new HashSet<>();
	private boolean containsUnsupportedFeature = false;

	TriplesPathWalker(UUIDGenerator uuidGenerator) {
		this.uuidGenerator = uuidGenerator;
	}

	List<List<Triple>> walk(Path path, List<List<Triple>> tripleCollections, Node start, Node end) {
		if (path instanceof P_Link) {
			return walk((P_Link) path, tripleCollections, start, end);
		} else if (path instanceof P_NegPropSet) {
			return walk((P_NegPropSet) path, tripleCollections, start, end);
		} else if (path instanceof P_Inverse) {
			return walk((P_Inverse) path, tripleCollections, start, end);
		} else if (path instanceof P_ZeroOrOne) {
			return walk((P_ZeroOrOne) path, tripleCollections, start, end);
		} else if (path instanceof P_ZeroOrMore1) {
			return walk((P_ZeroOrMore1) path, tripleCollections, start, end);
		} else if (path instanceof P_OneOrMore1) {
			return walk((P_OneOrMore1) path, tripleCollections, start, end);
		} else if (path instanceof P_Alt) {
			return walk((P_Alt) path, tripleCollections, start, end);
		} else if (path instanceof P_Seq) {
			return walk((P_Seq) path, tripleCollections, start, end);
		} else {
			System.out.println("Unknown path type encountered!");
			System.out.println(path);
			containsUnsupportedFeature = true;
			return tripleCollections;
		}
	}

	private List<List<Triple>> walk(P_Link path, List<List<Triple>> tripleCollections, Node start, Node end) {
//		encounteredFeatures.add(PathFeature.LINK);

		tripleCollections.forEach(collection -> collection.add(new Triple(start, path.getNode(), end)));
		return tripleCollections;
	}

	private List<List<Triple>> walk(P_NegPropSet path, List<List<Triple>> tripleCollections, Node start, Node end) {
		encounteredFeatures.add(PathFeature.NEGATED_PROP_SET);
		containsUnsupportedFeature = true;

		return tripleCollections;
	}

	private List<List<Triple>> walk(P_Inverse path, List<List<Triple>> tripleCollections, Node start, Node end) {
		encounteredFeatures.add(PathFeature.INVERSE);

		return walk(path.getSubPath(), tripleCollections, end, start);
	}

	private List<List<Triple>> walk(P_ZeroOrOne path, List<List<Triple>> tripleCollections, Node start, Node end) {
		encounteredFeatures.add(PathFeature.ZERO_OR_ONE);

		List<List<Triple>> appliedOnceCollections = tripleCollections.stream().map(ArrayList::new).collect(Collectors.toList());
		appliedOnceCollections = walk(path.getSubPath(), appliedOnceCollections, start, end);

		return Stream.concat(tripleCollections.stream(), appliedOnceCollections.stream()).collect(Collectors.toList());
	}

	private List<List<Triple>> walk(P_ZeroOrMore1 path, List<List<Triple>> tripleCollections, Node start, Node end) {
		encounteredFeatures.add(PathFeature.ZERO_OR_MORE);

		List<List<Triple>> appliedOnceCollections = tripleCollections.stream().map(ArrayList::new).collect(Collectors.toList());
		appliedOnceCollections = walk(path.getSubPath(), appliedOnceCollections, start, end);

		List<List<Triple>> appliedTwiceCollections = tripleCollections.stream().map(ArrayList::new).collect(Collectors.toList());
		Node center = new Node_Variable(uuidGenerator.generateUUID());
		appliedTwiceCollections = walk(path.getSubPath(), appliedTwiceCollections, start, center);
		appliedTwiceCollections = walk(path.getSubPath(), appliedTwiceCollections, center, end);

		return Stream.concat(tripleCollections.stream(), Stream.concat(appliedOnceCollections.stream(), appliedTwiceCollections.stream())).collect(Collectors.toList());
	}

	private List<List<Triple>> walk(P_OneOrMore1 path, List<List<Triple>> tripleCollections, Node start, Node end) {
		encounteredFeatures.add(PathFeature.ONE_OR_MORE);

		List<List<Triple>> appliedOnceCollections = tripleCollections.stream().map(ArrayList::new).collect(Collectors.toList());
		appliedOnceCollections = walk(path.getSubPath(), appliedOnceCollections, start, end);

		List<List<Triple>> appliedTwiceCollections = tripleCollections.stream().map(ArrayList::new).collect(Collectors.toList());
		Node center = new Node_Variable(uuidGenerator.generateUUID());
		appliedTwiceCollections = walk(path.getSubPath(), appliedTwiceCollections, start, center);
		appliedTwiceCollections = walk(path.getSubPath(), appliedTwiceCollections, center, end);

		return Stream.concat(appliedOnceCollections.stream(), appliedTwiceCollections.stream()).collect(Collectors.toList());
	}

	private List<List<Triple>> walk(P_Alt path, List<List<Triple>> tripleCollections, Node start, Node end) {
		encounteredFeatures.add(PathFeature.ALT);

		List<List<Triple>> leftCollections = tripleCollections.stream().map(ArrayList::new).collect(Collectors.toList());
		List<List<Triple>> rightCollections = tripleCollections.stream().map(ArrayList::new).collect(Collectors.toList());

		leftCollections = walk(path.getLeft(), leftCollections, start, end);
		rightCollections = walk(path.getRight(), rightCollections, start, end);

		return Stream.concat(leftCollections.stream(), rightCollections.stream()).collect(Collectors.toList());
	}

	private List<List<Triple>> walk(P_Seq path, List<List<Triple>> tripleCollections, Node start, Node end) {
		encounteredFeatures.add(PathFeature.SEQ);

		Node center = new Node_Variable(uuidGenerator.generateUUID());
		tripleCollections = walk(path.getLeft(), tripleCollections, start, center);
		tripleCollections = walk(path.getRight(), tripleCollections, center, end);
		return tripleCollections;
	}

	Set<PathFeature> getEncounteredFeatures() {
		return encounteredFeatures;
	}

	boolean containsUnsupportedFeature() {
		return containsUnsupportedFeature;
	}
}
