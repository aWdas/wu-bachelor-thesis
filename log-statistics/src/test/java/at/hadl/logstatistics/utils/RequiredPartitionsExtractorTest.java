package at.hadl.logstatistics.utils;

import at.hadl.logstatistics.utils.graphbuilding.LabeledEdge;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static at.hadl.logstatistics.utils.RequiredPartitionsExtractor.extractStarShapes;
import static org.assertj.core.api.Assertions.assertThat;

class RequiredPartitionsExtractorTest {
    @Test
    void oneGraphSimplePartitionsTest() {
        var sourceGraph = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        sourceGraph.addVertex("?a");
        sourceGraph.addVertex("?b");
        sourceGraph.addVertex("?c");
        sourceGraph.addVertex("?d");
        sourceGraph.addVertex("?e");
        sourceGraph.addVertex("?f");

        sourceGraph.addEdge("?a", "?b", new LabeledEdge(1));
        sourceGraph.addEdge("?a", "?c", new LabeledEdge(2));
        sourceGraph.addEdge("?b", "?c", new LabeledEdge(1));
        sourceGraph.addEdge("?c", "?d", new LabeledEdge(3));
        sourceGraph.addEdge("?d", "?e", new LabeledEdge(4));
        sourceGraph.addEdge("?d", "?f", new LabeledEdge(2));

        var graphs = Collections.singletonList(sourceGraph);

        String partitionsResult = extractStarShapes(graphs);

        assertThat(partitionsResult).isEqualTo("[\"1,2\",\"1\",\"3\",\"2,4\"]");
    }

    @Test
    void multipleGraphsDuplicatePartitionsTest() {
        var sourceGraph1 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        sourceGraph1.addVertex("?a");
        sourceGraph1.addVertex("?b");
        sourceGraph1.addVertex("?c");
        sourceGraph1.addVertex("?d");
        sourceGraph1.addVertex("?e");
        sourceGraph1.addVertex("?f");

        sourceGraph1.addEdge("?a", "?b", new LabeledEdge(1));
        sourceGraph1.addEdge("?a", "?c", new LabeledEdge(2));
        sourceGraph1.addEdge("?b", "?c", new LabeledEdge(1));
        sourceGraph1.addEdge("?c", "?d", new LabeledEdge(3));
        sourceGraph1.addEdge("?d", "?e", new LabeledEdge(4));
        sourceGraph1.addEdge("?d", "?f", new LabeledEdge(2));

        var sourceGraph2 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        sourceGraph2.addVertex("?a");
        sourceGraph2.addVertex("?b");
        sourceGraph2.addVertex("?c");
        sourceGraph2.addVertex("?d");
        sourceGraph2.addVertex("?e");

        sourceGraph2.addEdge("?a", "?b", new LabeledEdge(4));
        sourceGraph2.addEdge("?a", "?c", new LabeledEdge(2));
        sourceGraph2.addEdge("?b", "?c", new LabeledEdge(3));
        sourceGraph2.addEdge("?c", "?d", new LabeledEdge(7));
        sourceGraph2.addEdge("?c", "?e", new LabeledEdge(3));

        var graphs = Arrays.asList(sourceGraph1, sourceGraph2);

        String partitionsResult = extractStarShapes(graphs);

        assertThat(partitionsResult).isEqualTo("[\"1,2\",\"1\",\"3\",\"2,4\",\"3,7\"]");
    }
}
