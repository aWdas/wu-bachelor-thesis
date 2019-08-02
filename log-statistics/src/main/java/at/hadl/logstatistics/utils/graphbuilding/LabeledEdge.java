package at.hadl.logstatistics.utils.graphbuilding;

import org.jgrapht.graph.DefaultEdge;

import java.util.Objects;

public class LabeledEdge extends DefaultEdge {
    private int predicate;

    LabeledEdge(int predicate) {
        this.predicate = predicate;
    }

    public int getPredicate() {
        return predicate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LabeledEdge that = (LabeledEdge) o;
        return predicate == that.predicate && getSource().equals(that.getSource()) && getTarget().equals(that.getTarget());
    }

    @Override
    public int hashCode() {
        return Objects.hash(predicate);
    }

    @Override
    public String toString() {
        return "(" + getSource() + " : " + getTarget() + " : " + predicate + ")";
    }
}
