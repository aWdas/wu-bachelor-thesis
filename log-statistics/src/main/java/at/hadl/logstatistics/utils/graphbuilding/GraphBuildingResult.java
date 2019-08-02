package at.hadl.logstatistics.utils.graphbuilding;

import org.jgrapht.graph.DefaultDirectedGraph;

import java.util.List;
import java.util.Set;

public class GraphBuildingResult {
    private List<DefaultDirectedGraph<String, LabeledEdge>> constructedGraphs;
    private Set<String> encounteredFeatures;

    GraphBuildingResult(List<DefaultDirectedGraph<String, LabeledEdge>> constructedGraphs, Set<String> encounteredFeatures) {
        this.constructedGraphs = constructedGraphs;
        this.encounteredFeatures = encounteredFeatures;
    }

    public List<DefaultDirectedGraph<String, LabeledEdge>> getConstructedGraphs() {
        return constructedGraphs;
    }

    public Set<String> getEncounteredFeatures() {
        return encounteredFeatures;
    }
}
