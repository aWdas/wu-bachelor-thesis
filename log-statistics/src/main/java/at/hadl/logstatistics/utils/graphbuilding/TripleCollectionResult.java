package at.hadl.logstatistics.utils.graphbuilding;

import lombok.Builder;
import lombok.Getter;
import org.apache.jena.graph.Triple;

import java.util.List;

@Builder
@Getter
public class TripleCollectionResult {
    List<List<Triple>> mainQueryGraphs;
    List<List<Triple>> additionalQueryGraphs;
}
