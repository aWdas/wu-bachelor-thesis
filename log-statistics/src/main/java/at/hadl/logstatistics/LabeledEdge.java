package at.hadl.logstatistics;

import org.jgrapht.graph.DefaultEdge;

public class LabeledEdge extends DefaultEdge {
    private int predicate;

    public LabeledEdge(int predicate) {
        this.predicate = predicate;
    }

    public int getPredicate() {
        return predicate;
    }
}
