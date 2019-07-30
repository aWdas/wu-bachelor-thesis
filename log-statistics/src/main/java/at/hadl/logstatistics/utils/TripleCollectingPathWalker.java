package at.hadl.logstatistics.utils;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Variable;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.path.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TripleCollectingPathWalker {
	UUIDGenerator uuidGenerator = new UUIDGenerator();

	public List<List<Triple>> walk(Path path, List<List<Triple>> tripleCollections, Node start, Node end) {
		if (path instanceof P_Link) {
			return walk((P_Link) path, tripleCollections, start, end);
		} else if (path instanceof P_ReverseLink) {
			return walk((P_ReverseLink) path, tripleCollections, start, end);
		} else if (path instanceof P_NegPropSet) {
			return walk((P_NegPropSet) path, tripleCollections, start, end);
		} else if (path instanceof P_Inverse) {
			return walk((P_Inverse) path, tripleCollections, start, end);
		} else if (path instanceof P_Mod) {
			return walk((P_Mod) path, tripleCollections, start, end);
		} else if (path instanceof P_FixedLength) {
			return walk((P_FixedLength) path, tripleCollections, start, end);
		} else if (path instanceof P_Distinct) {
			return walk((P_Distinct) path, tripleCollections, start, end);
		} else if (path instanceof P_Multi) {
			return walk((P_Multi) path, tripleCollections, start, end);
		} else if (path instanceof P_Shortest) {
			return walk((P_Shortest) path, tripleCollections, start, end);
		} else if (path instanceof P_ZeroOrOne) {
			return walk((P_ZeroOrOne) path, tripleCollections, start, end);
		} else if (path instanceof P_ZeroOrMore1) {
			return walk((P_ZeroOrMore1) path, tripleCollections, start, end);
		} else if (path instanceof P_ZeroOrMoreN) {
			return walk((P_ZeroOrMoreN) path, tripleCollections, start, end);
		} else if (path instanceof P_OneOrMore1) {
			return walk((P_OneOrMore1) path, tripleCollections, start, end);
		} else if (path instanceof P_OneOrMoreN) {
			return walk((P_OneOrMoreN) path, tripleCollections, start, end);
		} else if (path instanceof P_Alt) {
			return walk((P_Alt) path, tripleCollections, start, end);
		} else if (path instanceof P_Seq) {
			return walk((P_Seq) path, tripleCollections, start, end);
		} else {
			throw new RuntimeException("Unknown path type!");
		}
	}

	private List<List<Triple>> walk(P_Link path, List<List<Triple>> tripleCollections, Node start, Node end) {
		System.out.println(path.getClass());
		System.out.println(path);

		tripleCollections.forEach(collection -> collection.add(new Triple(start, path.getNode(), end)));
		return tripleCollections;
	}

	private List<List<Triple>> walk(P_ReverseLink path, List<List<Triple>> tripleCollections, Node start, Node end) {
		System.out.println(path.getClass());
		System.out.println(path);
		return tripleCollections;
	}

	private List<List<Triple>> walk(P_NegPropSet path, List<List<Triple>> tripleCollections, Node start, Node end) {
		System.out.println(path.getClass());
		System.out.println(path);
		return tripleCollections;
	}

	private List<List<Triple>> walk(P_Inverse path, List<List<Triple>> tripleCollections, Node start, Node end) {
		System.out.println(path.getClass());
		System.out.println(path);
		return tripleCollections;
	}

	private List<List<Triple>> walk(P_Mod path, List<List<Triple>> tripleCollections, Node start, Node end) {
		System.out.println(path.getClass());
		System.out.println(path);
		return tripleCollections;
	}

	private List<List<Triple>> walk(P_FixedLength path, List<List<Triple>> tripleCollections, Node start, Node end) {
		System.out.println(path.getClass());
		System.out.println(path);
		return tripleCollections;
	}

	private List<List<Triple>> walk(P_Distinct path, List<List<Triple>> tripleCollections, Node start, Node end) {
		System.out.println(path.getClass());
		System.out.println(path);
		return tripleCollections;
	}

	private List<List<Triple>> walk(P_Multi path, List<List<Triple>> tripleCollections, Node start, Node end) {
		System.out.println(path.getClass());
		System.out.println(path);
		return tripleCollections;
	}

	private List<List<Triple>> walk(P_Shortest path, List<List<Triple>> tripleCollections, Node start, Node end) {
		System.out.println(path.getClass());
		System.out.println(path);
		return tripleCollections;
	}

	private List<List<Triple>> walk(P_ZeroOrOne path, List<List<Triple>> tripleCollections, Node start, Node end) {
		System.out.println(path.getClass());
		System.out.println(path);
		return tripleCollections;
	}

	private List<List<Triple>> walk(P_ZeroOrMore1 path, List<List<Triple>> tripleCollections, Node start, Node end) {
		System.out.println(path.getClass());
		System.out.println(path);

		List<List<Triple>> appliedOnceCollections = tripleCollections.stream().map(ArrayList::new).collect(Collectors.toList());
		appliedOnceCollections = walk(path.getSubPath(), appliedOnceCollections, start, end);

		List<List<Triple>> appliedTwiceCollections = tripleCollections.stream().map(ArrayList::new).collect(Collectors.toList());
		Node center = new Node_Variable(uuidGenerator.generateUUID());
		appliedTwiceCollections = walk(path.getSubPath(), appliedTwiceCollections, start, center);
		appliedTwiceCollections = walk(path.getSubPath(), appliedTwiceCollections, center, end);

		return Stream.concat(Stream.concat(tripleCollections.stream(), appliedOnceCollections.stream()), appliedTwiceCollections.stream()).collect(Collectors.toList());
	}

	private List<List<Triple>> walk(P_ZeroOrMoreN path, List<List<Triple>> tripleCollections, Node start, Node end) {
		System.out.println(path.getClass());
		System.out.println(path);
		return tripleCollections;
	}

	private List<List<Triple>> walk(P_OneOrMore1 path, List<List<Triple>> tripleCollections, Node start, Node end) {
		System.out.println(path.getClass());
		System.out.println(path);

		List<List<Triple>> appliedOnceCollections = tripleCollections.stream().map(ArrayList::new).collect(Collectors.toList());
		appliedOnceCollections = walk(path.getSubPath(), appliedOnceCollections, start, end);

		List<List<Triple>> appliedTwiceCollections = tripleCollections.stream().map(ArrayList::new).collect(Collectors.toList());
		Node center = new Node_Variable(uuidGenerator.generateUUID());
		appliedTwiceCollections = walk(path.getSubPath(), appliedTwiceCollections, start, center);
		appliedTwiceCollections = walk(path.getSubPath(), appliedTwiceCollections, center, end);

		return Stream.concat(appliedOnceCollections.stream(), appliedTwiceCollections.stream()).collect(Collectors.toList());
	}

	private List<List<Triple>> walk(P_OneOrMoreN path, List<List<Triple>> tripleCollections, Node start, Node end) {
		System.out.println(path.getClass());
		System.out.println(path);
		return tripleCollections;
	}

	private List<List<Triple>> walk(P_Alt path, List<List<Triple>> tripleCollections, Node start, Node end) {
		System.out.println(path.getClass());
		System.out.println(path);

		List<List<Triple>> leftCollections = tripleCollections.stream().map(ArrayList::new).collect(Collectors.toList());
		List<List<Triple>> rightCollections = tripleCollections.stream().map(ArrayList::new).collect(Collectors.toList());

		leftCollections = walk(path.getLeft(), leftCollections, start, end);
		rightCollections = walk(path.getRight(), rightCollections, start, end);

		return Stream.concat(leftCollections.stream(), rightCollections.stream()).collect(Collectors.toList());
	}

	private List<List<Triple>> walk(P_Seq path, List<List<Triple>> tripleCollections, Node start, Node end) {
		System.out.println(path.getClass());
		System.out.println(path);

		Node center = new Node_Variable(uuidGenerator.generateUUID());
		tripleCollections = walk(path.getLeft(), tripleCollections, start, center);
		tripleCollections = walk(path.getRight(), tripleCollections, center, end);
		return tripleCollections;
	}

	public void setUuidGenerator(UUIDGenerator uuidGenerator) {
		this.uuidGenerator = uuidGenerator;
	}
}
