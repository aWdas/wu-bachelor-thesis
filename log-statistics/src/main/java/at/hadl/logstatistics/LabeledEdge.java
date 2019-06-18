package at.hadl.logstatistics;

import org.jgrapht.graph.DefaultEdge;

public class LabeledEdge extends DefaultEdge {
    private String predicate;

    public LabeledEdge(String predicate) {
        this.predicate = predicate;
    }

    public String getPredicate() {
        return predicate;
    }

    @Override
    public String toString() {
        return getPredicate();
    }
}
