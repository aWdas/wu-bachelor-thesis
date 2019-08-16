package at.hadl.logstatistics.utils.graphbuilding;

import at.hadl.logstatistics.utils.PredicateMap;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

class GraphBuilderTest {
    private static final String PREFIX = "PREFIX  : <http://xmlns.com/foaf/0.1/>\n";
    private static final PredicateMap PREDICATE_MAP = new PredicateMap();

    private GraphBuilder graphBuilder;

    @BeforeAll
    public static void setUpAll() {
        PREDICATE_MAP.getPredicateMap().put("http://xmlns.com/foaf/0.1/knows", 1);
        PREDICATE_MAP.getPredicateMap().put("http://xmlns.com/foaf/0.1/foaf", 2);
        PREDICATE_MAP.getPredicateMap().put("http://xmlns.com/foaf/0.1/name", 3);
        PREDICATE_MAP.getPredicateMap().put("http://xmlns.com/foaf/0.1/worksAt", 4);
        PREDICATE_MAP.getPredicateMap().put("http://xmlns.com/foaf/0.1/companyName", 5);
    }

    @BeforeEach
    public void setUp() {
        var uUIDGenerator = Mockito.mock(UUIDGenerator.class);
        TriplesElementWalkerFactory triplesElementWalkerFactory = new TriplesElementWalkerFactory(uUIDGenerator);
        graphBuilder = new GraphBuilder(triplesElementWalkerFactory);

        var counter = new AtomicInteger(0);
        given(uUIDGenerator.generateUUID()).willAnswer((invocationOnMock) -> "uuid" + counter.incrementAndGet());
    }

    @Test
    void noSpecialOperatorsTest() {
        var queryString = PREFIX + "SELECT ?a WHERE { " +
                "?a :knows ?b . " +
                "?a :foaf ?c . " +
                "?a :name \"Karl\" " +
                " }";

        var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
        var graphBuildingResult = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

        var expectedGraph = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph.addVertex("?a");
        expectedGraph.addVertex("?b");
        expectedGraph.addVertex("?c");
        expectedGraph.addVertex("\"Karl\"");

        expectedGraph.addEdge("?a", "?b", new LabeledEdge(1));
        expectedGraph.addEdge("?a", "?c", new LabeledEdge(2));
        expectedGraph.addEdge("?a", "\"Karl\"", new LabeledEdge(3));

        assertThat(graphBuildingResult.getConstructedGraphs()).containsOnly(expectedGraph);
        assertThat(graphBuildingResult.getEncounteredFeatures()).isEmpty();
    }

    @Test
    void optionalOnlyTest() {
        var queryString = PREFIX + "SELECT ?a WHERE { " +
                "?a :knows ?b . " +
                "?a :foaf ?c . " +
                "OPTIONAL { " +
                "   ?a :worksAt ?company ." +
                "   ?company :companyName \"Some Company\"" +
                "} }";

        var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
        var graphBuildingResult = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

        var expectedGraph1 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph1.addVertex("?a");
        expectedGraph1.addVertex("?b");
        expectedGraph1.addVertex("?c");
        expectedGraph1.addEdge("?a", "?b", new LabeledEdge(1));
        expectedGraph1.addEdge("?a", "?c", new LabeledEdge(2));

        var expectedGraph2 = (DefaultDirectedGraph<String, LabeledEdge>) expectedGraph1.clone();
        expectedGraph2.addVertex("?company");
        expectedGraph2.addVertex("\"Some Company\"");
        expectedGraph2.addEdge("?a", "?company", new LabeledEdge(4));
        expectedGraph2.addEdge("?company", "\"Some Company\"", new LabeledEdge(5));

        assertThat(graphBuildingResult.getConstructedGraphs()).containsOnly(expectedGraph1, expectedGraph2);
        assertThat(graphBuildingResult.getEncounteredFeatures()).containsOnly(QueryFeature.OPTIONAL.name());
    }

    @Test
    void unionOnlyTest() {
        var queryString = PREFIX + "SELECT ?a WHERE { " +
                "?a :knows ?b . " +
                "{ ?a :foaf ?c } " +
                "UNION { " +
                "   ?a :worksAt ?company ." +
                "   ?company :companyName \"Some Company\"" +
                "} }";

        var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
        var graphBuildingResult = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

        var expectedGraph1 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph1.addVertex("?a");
        expectedGraph1.addVertex("?b");
        expectedGraph1.addEdge("?a", "?b", new LabeledEdge(1));

        var expectedGraph2 = (DefaultDirectedGraph<String, LabeledEdge>) expectedGraph1.clone();

        expectedGraph1.addVertex("?c");
        expectedGraph1.addEdge("?a", "?c", new LabeledEdge(2));

        expectedGraph2.addVertex("?company");
        expectedGraph2.addVertex("\"Some Company\"");
        expectedGraph2.addEdge("?a", "?company", new LabeledEdge(4));
        expectedGraph2.addEdge("?company", "\"Some Company\"", new LabeledEdge(5));

        assertThat(graphBuildingResult.getConstructedGraphs()).containsOnly(expectedGraph1, expectedGraph2);
        assertThat(graphBuildingResult.getEncounteredFeatures()).containsOnly(QueryFeature.UNION.name());
    }

    @Test
    void filterNotExistsOnlyTest() {
        var queryString = PREFIX + "SELECT ?a WHERE { " +
                "?a :knows ?b . " +
                "{ ?a :foaf ?c } ." +
                "FILTER (STRSTARTS(?a, \"jdoe\") && NOT EXISTS { " +
                "   ?a :worksAt ?company ." +
                "   ?company :companyName \"Some Company\"" +
                "}) }";

        var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
        var graphBuildingResult = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

        var expectedGraph1 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph1.addVertex("?a");
        expectedGraph1.addVertex("?b");
        expectedGraph1.addVertex("?c");
        expectedGraph1.addEdge("?a", "?b", new LabeledEdge(1));
        expectedGraph1.addEdge("?a", "?c", new LabeledEdge(2));

        var expectedGraph2 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph2.addVertex("?a");
        expectedGraph2.addVertex("?company");
        expectedGraph2.addVertex("\"Some Company\"");
        expectedGraph2.addEdge("?a", "?company", new LabeledEdge(4));
        expectedGraph2.addEdge("?company", "\"Some Company\"", new LabeledEdge(5));

        assertThat(graphBuildingResult.getConstructedGraphs()).containsOnly(expectedGraph1, expectedGraph2);
        assertThat(graphBuildingResult.getEncounteredFeatures()).containsOnly(QueryFeature.FILTER.name(), QueryFeature.FILTER_NOT_EXISTS.name());
    }

    @Test
    void filterExistsOnlyTest() {
        var queryString = PREFIX + "SELECT ?a WHERE { " +
                "?a :knows ?b . " +
                "{ ?a :foaf ?c } ." +
                "FILTER (STRSTARTS(?a, \"jdoe\") && EXISTS { " +
                "   ?a :worksAt ?company ." +
                "   ?company :companyName \"Some Company\"" +
                "}) }";

        var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
        var graphBuildingResult = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

        var expectedGraph1 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph1.addVertex("?a");
        expectedGraph1.addVertex("?b");
        expectedGraph1.addVertex("?c");
        expectedGraph1.addEdge("?a", "?b", new LabeledEdge(1));
        expectedGraph1.addEdge("?a", "?c", new LabeledEdge(2));

        var expectedGraph2 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph2.addVertex("?a");
        expectedGraph2.addVertex("?company");
        expectedGraph2.addVertex("\"Some Company\"");
        expectedGraph2.addEdge("?a", "?company", new LabeledEdge(4));
        expectedGraph2.addEdge("?company", "\"Some Company\"", new LabeledEdge(5));

        assertThat(graphBuildingResult.getConstructedGraphs()).containsOnly(expectedGraph1, expectedGraph2);
        assertThat(graphBuildingResult.getEncounteredFeatures()).containsOnly(QueryFeature.FILTER.name(), QueryFeature.FILTER_EXISTS.name());
    }

    @Test
    void minusOnlyTest() {
        var queryString = PREFIX + "SELECT ?a WHERE { " +
                "?a :knows ?b . " +
                "{ ?a :foaf ?c } . " +
                "MINUS { " +
                "   ?a :worksAt ?company ." +
                "   ?company :companyName \"Some Company\"" +
                "} }";

        var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
        var graphBuildingResult = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

        var expectedGraph1 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph1.addVertex("?a");
        expectedGraph1.addVertex("?b");
        expectedGraph1.addVertex("?c");
        expectedGraph1.addEdge("?a", "?b", new LabeledEdge(1));
        expectedGraph1.addEdge("?a", "?c", new LabeledEdge(2));

        var expectedGraph2 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph2.addVertex("?a");
        expectedGraph2.addVertex("?company");
        expectedGraph2.addVertex("\"Some Company\"");
        expectedGraph2.addEdge("?a", "?company", new LabeledEdge(4));
        expectedGraph2.addEdge("?company", "\"Some Company\"", new LabeledEdge(5));

        assertThat(graphBuildingResult.getConstructedGraphs()).containsOnly(expectedGraph1, expectedGraph2);
        assertThat(graphBuildingResult.getEncounteredFeatures()).containsOnly(QueryFeature.MINUS.name());
    }

    @Test
    void subqueryTest() {
        var queryString = PREFIX + "SELECT ?a WHERE { " +
                "?a :knows ?b . " +
                "OPTIONAL { ?b :name \"someOtherName\" } . " +
                "{ SELECT ?b WHERE { " +
                "   ?b :worksAt ?c ." +
                "   OPTIONAL { ?c :companyName \"someName\" } " +
                "} } . " +
                "}";

        var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
        var graphBuildingResult = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

        var expectedGraph1 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph1.addVertex("?a");
        expectedGraph1.addVertex("?b");
        expectedGraph1.addEdge("?a", "?b", new LabeledEdge(1));

        var expectedGraph2 = (DefaultDirectedGraph<String, LabeledEdge>) expectedGraph1.clone();
        expectedGraph2.addVertex("\"someOtherName\"");
        expectedGraph2.addEdge("?b", "\"someOtherName\"", new LabeledEdge(3));

        var expectedGraph3 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph3.addVertex("?b");
        expectedGraph3.addVertex("?c");
        expectedGraph3.addEdge("?b", "?c", new LabeledEdge(4));

        var expectedGraph4 = (DefaultDirectedGraph<String, LabeledEdge>) expectedGraph3.clone();
        expectedGraph4.addVertex("\"someName\"");
        expectedGraph4.addEdge("?c", "\"someName\"", new LabeledEdge(5));

        assertThat(graphBuildingResult.getConstructedGraphs()).containsOnly(expectedGraph1, expectedGraph2, expectedGraph3, expectedGraph4);
        assertThat(graphBuildingResult.getEncounteredFeatures()).containsOnly(QueryFeature.SUB_QUERY.name(), QueryFeature.OPTIONAL.name());
    }

    @Test
    void orPropertyPathOnlyTest() {
        var queryString = PREFIX + "SELECT ?a WHERE { " +
                "?a :knows ?b . " +
                "?a :foaf | :worksAt ?c . " +
                " }";

        var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
        var graphBuildingResult = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

        var expectedGraph1 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph1.addVertex("?a");
        expectedGraph1.addVertex("?b");
        expectedGraph1.addVertex("?c");
        expectedGraph1.addEdge("?a", "?b", new LabeledEdge(1));

        var expectedGraph2 = (DefaultDirectedGraph<String, LabeledEdge>) expectedGraph1.clone();

        expectedGraph1.addEdge("?a", "?c", new LabeledEdge(2));
        expectedGraph2.addEdge("?a", "?c", new LabeledEdge(4));

        assertThat(graphBuildingResult.getConstructedGraphs()).containsOnly(expectedGraph1, expectedGraph2);
        assertThat(graphBuildingResult.getEncounteredFeatures()).containsOnly(QueryFeature.PROPERTY_PATH.name(), PathFeature.ALT.name());
    }

    @Test
    void orAndSequencePropertyPathTest() {
        var queryString = PREFIX + "SELECT ?a WHERE { " +
                "?a :knows ?b . " +
                "?a :foaf / :name | :worksAt ?c . " +
                " }";

        var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
        var graphBuildingResult = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

        var expectedGraph1 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph1.addVertex("?a");
        expectedGraph1.addVertex("?b");
        expectedGraph1.addVertex("?c");
        expectedGraph1.addEdge("?a", "?b", new LabeledEdge(1));

        var expectedGraph2 = (DefaultDirectedGraph<String, LabeledEdge>) expectedGraph1.clone();

        expectedGraph1.addVertex("?uuid1");
        expectedGraph1.addEdge("?a", "?uuid1", new LabeledEdge(2));
        expectedGraph1.addEdge("?uuid1", "?c", new LabeledEdge(3));
        expectedGraph2.addEdge("?a", "?c", new LabeledEdge(4));

        assertThat(graphBuildingResult.getConstructedGraphs()).containsOnly(expectedGraph1, expectedGraph2);
        assertThat(graphBuildingResult.getEncounteredFeatures()).containsOnly(QueryFeature.PROPERTY_PATH.name(), PathFeature.SEQ.name(), PathFeature.ALT.name());
    }

    @Test
    void zeroOrMorePropertyPathOnlyTest() {
        var queryString = PREFIX + "SELECT ?a WHERE { " +
                "?a :knows ?b . " +
                "?a :foaf* ?c . " +
                " }";

        var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
        var graphBuildingResult = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

        var expectedGraph1 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph1.addVertex("?a");
        expectedGraph1.addVertex("?b");
        expectedGraph1.addEdge("?a", "?b", new LabeledEdge(1));

        var expectedGraph2 = (DefaultDirectedGraph<String, LabeledEdge>) expectedGraph1.clone();
        expectedGraph2.addVertex("?c");
        expectedGraph2.addEdge("?a", "?c", new LabeledEdge(2));

        var expectedGraph3 = (DefaultDirectedGraph<String, LabeledEdge>) expectedGraph1.clone();

        expectedGraph3.addVertex("?c");
        expectedGraph3.addVertex("?uuid1");
        expectedGraph3.addEdge("?a", "?uuid1", new LabeledEdge(2));
        expectedGraph3.addEdge("?uuid1", "?c", new LabeledEdge(2));

        assertThat(graphBuildingResult.getConstructedGraphs()).containsOnly(expectedGraph1, expectedGraph2, expectedGraph3);
        assertThat(graphBuildingResult.getEncounteredFeatures()).containsOnly(QueryFeature.PROPERTY_PATH.name(), PathFeature.ZERO_OR_MORE.name());
    }

    @Test
    void oneOrMorePropertyPathOnlyTest() {
        var queryString = PREFIX + "SELECT ?a WHERE { " +
                "?a :knows ?b . " +
                "?a :foaf+ ?c . " +
                " }";

        var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
        var graphBuildingResult = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

        var expectedGraph1 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph1.addVertex("?a");
        expectedGraph1.addVertex("?b");
        expectedGraph1.addVertex("?c");
        expectedGraph1.addEdge("?a", "?b", new LabeledEdge(1));

        var expectedGraph2 = (DefaultDirectedGraph<String, LabeledEdge>) expectedGraph1.clone();

        expectedGraph1.addEdge("?a", "?c", new LabeledEdge(2));
        expectedGraph2.addVertex("?uuid1");
        expectedGraph2.addEdge("?a", "?uuid1", new LabeledEdge(2));
        expectedGraph2.addEdge("?uuid1", "?c", new LabeledEdge(2));

        assertThat(graphBuildingResult.getConstructedGraphs()).containsOnly(expectedGraph1, expectedGraph2);
        assertThat(graphBuildingResult.getEncounteredFeatures()).containsOnly(QueryFeature.PROPERTY_PATH.name(), PathFeature.ONE_OR_MORE.name());
    }

    @Test
    void zeroOrOnePropertyPathOnlyTest() {
        var queryString = PREFIX + "SELECT ?a WHERE { " +
                "?a :knows ?b . " +
                "?a :foaf? ?c . " +
                " }";

        var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
        var graphBuildingResult = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

        var expectedGraph1 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph1.addVertex("?a");
        expectedGraph1.addVertex("?b");
        expectedGraph1.addEdge("?a", "?b", new LabeledEdge(1));

        var expectedGraph2 = (DefaultDirectedGraph<String, LabeledEdge>) expectedGraph1.clone();

        expectedGraph2.addVertex("?c");
        expectedGraph2.addEdge("?a", "?c", new LabeledEdge(2));

        assertThat(graphBuildingResult.getConstructedGraphs()).containsOnly(expectedGraph1, expectedGraph2);
        assertThat(graphBuildingResult.getEncounteredFeatures()).containsOnly(QueryFeature.PROPERTY_PATH.name(), PathFeature.ZERO_OR_ONE.name());
    }

    @Test
    void negationPropertyPathOnlyTest() {
        var queryString = PREFIX + "SELECT ?a WHERE { " +
                "?a :knows ?b . " +
                "?a !:foaf ?c . " +
                " }";

        var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
        var graphBuildingResult = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

        assertThat(graphBuildingResult.getConstructedGraphs()).isEqualTo(Collections.emptyList());
        assertThat(graphBuildingResult.getEncounteredFeatures()).containsOnly(QueryFeature.PROPERTY_PATH.name(), PathFeature.NEGATED_PROP_SET.name());
    }

    @Test
    void inversePropertyPathOnlyTest() {
        var queryString = PREFIX + "SELECT ?a WHERE { " +
                "?a :knows ?b . " +
                "?a ^:foaf ?c . " +
                " }";

        var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
        var graphBuildingResult = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

        var expectedGraph1 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph1.addVertex("?a");
        expectedGraph1.addVertex("?b");
        expectedGraph1.addVertex("?c");
        expectedGraph1.addEdge("?a", "?b", new LabeledEdge(1));
        expectedGraph1.addEdge("?c", "?a", new LabeledEdge(2));

        assertThat(graphBuildingResult.getConstructedGraphs()).containsOnly(expectedGraph1);
        assertThat(graphBuildingResult.getEncounteredFeatures()).containsOnly(QueryFeature.PROPERTY_PATH.name(), PathFeature.INVERSE.name());
    }

    @Test
    void multipleOptionalTest() {
        var queryString = PREFIX + "SELECT ?a WHERE { " +
                "?a :knows ?b . " +
                "?a :foaf ?c . " +
                "OPTIONAL { " +
                "   ?a :worksAt ?company . " +
                "	OPTIONAL { " +
                "   	?company :companyName \"Some Company\" " +
                "	}" +
                "} . " +
                "OPTIONAL {" +
                "	?b :name \"John Doe\"" +
                "} . " +
                "?c :name ?cName }";

        var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
        var graphBuildingResult = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

        var expectedGraph1 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph1.addVertex("?a");
        expectedGraph1.addVertex("?b");
        expectedGraph1.addVertex("?c");
        expectedGraph1.addVertex("?cName");
        expectedGraph1.addEdge("?a", "?b", new LabeledEdge(1));
        expectedGraph1.addEdge("?a", "?c", new LabeledEdge(2));
        expectedGraph1.addEdge("?c", "?cName", new LabeledEdge(3));

        var expectedGraph2 = (DefaultDirectedGraph<String, LabeledEdge>) expectedGraph1.clone();
        expectedGraph2.addVertex("?company");
        expectedGraph2.addEdge("?a", "?company", new LabeledEdge(4));

        var expectedGraph3 = (DefaultDirectedGraph<String, LabeledEdge>) expectedGraph2.clone();
        expectedGraph3.addVertex("\"Some Company\"");
        expectedGraph3.addEdge("?company", "\"Some Company\"", new LabeledEdge(5));

        var expectedGraph4 = (DefaultDirectedGraph<String, LabeledEdge>) expectedGraph1.clone();
        expectedGraph4.addVertex("\"John Doe\"");
        expectedGraph4.addEdge("?b", "\"John Doe\"", new LabeledEdge(3));

        assertThat(graphBuildingResult.getConstructedGraphs()).containsOnly(expectedGraph1, expectedGraph2, expectedGraph3, expectedGraph4);
        assertThat(graphBuildingResult.getEncounteredFeatures()).containsOnly(QueryFeature.OPTIONAL.name());
    }

    @Test
    public void complicatedQuery1Test() {
        var queryString = PREFIX + "SELECT ?a WHERE { " +
                "?a :knows ?b . " +
                "{ " +
                "   ?a :foaf ?c . " +
                "} . " +
                "OPTIONAL { " +
                "   ?a :worksAt ?company . " +
                "   { " +
                "       ?b :name ?bName " +
                "   } UNION {" +
                "       ?b :worksAt ?bCompany " +
                "   } . " +
                "	OPTIONAL { " +
                "   	?company :companyName \"Some Company\" " +
                "	} . " +
                "   FILTER EXISTS { ?d :name ?dName } " +
                "} . " +
                "OPTIONAL {" +
                "	?b :name \"John Doe\"" +
                "} . " +
                "MINUS { " +
                "   ?a :name \"Blub Blobster\" . " +
                "   OPTIONAL { " +
                "       ?a :foaf ?aFriend " +
                "   } " +
                "} . " +
                "?c :name ?cName }";

        var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
        var graphBuildingResult = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

        var expectedGraph1 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph1.addVertex("?a");
        expectedGraph1.addVertex("?b");
        expectedGraph1.addVertex("?c");
        expectedGraph1.addVertex("?cName");
        expectedGraph1.addEdge("?a", "?b", new LabeledEdge(1));
        expectedGraph1.addEdge("?a", "?c", new LabeledEdge(2));
        expectedGraph1.addEdge("?c", "?cName", new LabeledEdge(3));

        var expectedGraph2 = (DefaultDirectedGraph<String, LabeledEdge>) expectedGraph1.clone();
        expectedGraph2.addVertex("?company");
        expectedGraph2.addEdge("?a", "?company", new LabeledEdge(4));

        var expectedGraph3 = (DefaultDirectedGraph<String, LabeledEdge>) expectedGraph2.clone();
        expectedGraph2.addVertex("?bName");
        expectedGraph2.addEdge("?b", "?bName", new LabeledEdge(3));

        expectedGraph3.addVertex("?bCompany");
        expectedGraph3.addEdge("?b", "?bCompany", new LabeledEdge(4));

        var expectedGraph4 = (DefaultDirectedGraph<String, LabeledEdge>) expectedGraph2.clone();
        expectedGraph4.addVertex("\"Some Company\"");
        expectedGraph4.addEdge("?company", "\"Some Company\"", new LabeledEdge(5));

        var expectedGraph5 = (DefaultDirectedGraph<String, LabeledEdge>) expectedGraph3.clone();
        expectedGraph5.addVertex("\"Some Company\"");
        expectedGraph5.addEdge("?company", "\"Some Company\"", new LabeledEdge(5));

        var expectedGraph6 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph6.addVertex("?d");
        expectedGraph6.addVertex("?dName");
        expectedGraph6.addEdge("?d", "?dName", new LabeledEdge(3));

        var expectedGraph7 = (DefaultDirectedGraph<String, LabeledEdge>) expectedGraph1.clone();
        expectedGraph7.addVertex("\"John Doe\"");
        expectedGraph7.addEdge("?b", "\"John Doe\"", new LabeledEdge(3));

        var expectedGraph8 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph8.addVertex("?a");
        expectedGraph8.addVertex("\"Blub Blobster\"");
        expectedGraph8.addEdge("?a", "\"Blub Blobster\"", new LabeledEdge(3));

        var expectedGraph9 = (DefaultDirectedGraph<String, LabeledEdge>) expectedGraph8.clone();
        expectedGraph9.addVertex("?aFriend");
        expectedGraph9.addEdge("?a", "?aFriend", new LabeledEdge(2));

        assertThat(graphBuildingResult.getConstructedGraphs()).containsOnly(expectedGraph1, expectedGraph2, expectedGraph3, expectedGraph4, expectedGraph5, expectedGraph6, expectedGraph7, expectedGraph8, expectedGraph9);
        assertThat(graphBuildingResult.getEncounteredFeatures()).containsOnly(QueryFeature.OPTIONAL.name(), QueryFeature.UNION.name(), QueryFeature.MINUS.name(), QueryFeature.FILTER.name(), QueryFeature.FILTER_EXISTS.name());
    }

    @Test
    public void complicatedQuery2Test() {
        var queryString = PREFIX + " SELECT ?a WHERE { " +
                "   ?a :knows / :foaf / (:worksAt? | :knows*) / :knows ?b ." +
                "   MINUS {" +
                "       ?a :worksAt \"SomeCompany\"" +
                "   }" +
                "}";

        var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
        var graphBuildingResult = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

        // option 1/2 for :worksAt? and 1/3 for :knows*
        var expectedGraph1 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph1.addVertex("?a");
        expectedGraph1.addVertex("?b");
        expectedGraph1.addVertex("?uuid1");
        expectedGraph1.addVertex("?uuid3");
        expectedGraph1.addEdge("?a", "?uuid3", new LabeledEdge(1));
        expectedGraph1.addEdge("?uuid3", "?uuid1", new LabeledEdge(2));
        expectedGraph1.addEdge("?uuid1", "?b", new LabeledEdge(1));

        // option 2/2 for :worksAt?
        var expectedGraph2 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph2.addVertex("?a");
        expectedGraph2.addVertex("?b");
        expectedGraph2.addVertex("?uuid1");
        expectedGraph2.addVertex("?uuid2");
        expectedGraph2.addVertex("?uuid3");
        expectedGraph2.addEdge("?a", "?uuid3", new LabeledEdge(1));
        expectedGraph2.addEdge("?uuid3", "?uuid2", new LabeledEdge(2));
        expectedGraph2.addEdge("?uuid2", "?uuid1", new LabeledEdge(4));
        expectedGraph2.addEdge("?uuid1", "?b", new LabeledEdge(1));

        // option 2/3 for :knows*
        var expectedGraph3 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph3.addVertex("?a");
        expectedGraph3.addVertex("?b");
        expectedGraph3.addVertex("?uuid1");
        expectedGraph3.addVertex("?uuid2");
        expectedGraph3.addVertex("?uuid3");
        expectedGraph3.addEdge("?a", "?uuid3", new LabeledEdge(1));
        expectedGraph3.addEdge("?uuid3", "?uuid2", new LabeledEdge(2));
        expectedGraph3.addEdge("?uuid2", "?uuid1", new LabeledEdge(1));
        expectedGraph3.addEdge("?uuid1", "?b", new LabeledEdge(1));

        // option 3/3 for :knows*
        var expectedGraph4 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph4.addVertex("?a");
        expectedGraph4.addVertex("?b");
        expectedGraph4.addVertex("?uuid1");
        expectedGraph4.addVertex("?uuid2");
        expectedGraph4.addVertex("?uuid3");
        expectedGraph4.addVertex("?uuid4");
        expectedGraph4.addEdge("?a", "?uuid3", new LabeledEdge(1));
        expectedGraph4.addEdge("?uuid3", "?uuid2", new LabeledEdge(2));
        expectedGraph4.addEdge("?uuid2", "?uuid4", new LabeledEdge(1));
        expectedGraph4.addEdge("?uuid4", "?uuid1", new LabeledEdge(1));
        expectedGraph4.addEdge("?uuid1", "?b", new LabeledEdge(1));

        // MINUS
        var expectedGraph5 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph5.addVertex("?a");
        expectedGraph5.addVertex("\"SomeCompany\"");
        expectedGraph5.addEdge("?a", "\"SomeCompany\"", new LabeledEdge(4));

        assertThat(graphBuildingResult.getConstructedGraphs()).containsOnly(expectedGraph1, expectedGraph1, expectedGraph2, expectedGraph3, expectedGraph4, expectedGraph5);
        assertThat(graphBuildingResult.getEncounteredFeatures()).containsOnly(QueryFeature.MINUS.name(), QueryFeature.PROPERTY_PATH.name(), PathFeature.ZERO_OR_MORE.name(), PathFeature.ALT.name(), PathFeature.ZERO_OR_ONE.name(), PathFeature.SEQ.name());
    }

    @Test
    public void complicatedQuery3Test() {
        var queryString = PREFIX + " SELECT ?a WHERE { " +
                "   ?a :knows / ^(:name+) / (:worksAt? / :foaf)? ?b . " +
                "}";

        var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
        var graphBuildingResult = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

        // :name once and (:worksAt? / :foaf) none
        var expectedGraph1 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph1.addVertex("?a");
        expectedGraph1.addVertex("?b");
        expectedGraph1.addVertex("?uuid2");
        expectedGraph1.addEdge("?a", "?uuid2", new LabeledEdge(1));
        expectedGraph1.addEdge("?b", "?uuid2", new LabeledEdge(3));

        // :name twice and (:worksAt? / :foaf) none
        var expectedGraph2 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph2.addVertex("?a");
        expectedGraph2.addVertex("?b");
        expectedGraph2.addVertex("?uuid2");
        expectedGraph2.addVertex("?uuid3");
        expectedGraph2.addEdge("?a", "?uuid2", new LabeledEdge(1));
        expectedGraph2.addEdge("?b", "?uuid3", new LabeledEdge(3));
        expectedGraph2.addEdge("?uuid3", "?uuid2", new LabeledEdge(3));

        // :name once and (:worksAt? / :foaf) once (:worksAt none)
        var expectedGraph3 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph3.addVertex("?a");
        expectedGraph3.addVertex("?b");
        expectedGraph3.addVertex("?uuid1");
        expectedGraph3.addVertex("?uuid2");
        expectedGraph3.addEdge("?a", "?uuid2", new LabeledEdge(1));
        expectedGraph3.addEdge("?uuid1", "?uuid2", new LabeledEdge(3));
        expectedGraph3.addEdge("?uuid1", "?b", new LabeledEdge(2));

        // :name once and (:worksAt? / :foaf) once (:worksAt once)
        var expectedGraph4 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph4.addVertex("?a");
        expectedGraph4.addVertex("?b");
        expectedGraph4.addVertex("?uuid1");
        expectedGraph4.addVertex("?uuid2");
        expectedGraph4.addVertex("?uuid4");
        expectedGraph4.addEdge("?a", "?uuid2", new LabeledEdge(1));
        expectedGraph4.addEdge("?uuid1", "?uuid2", new LabeledEdge(3));
        expectedGraph4.addEdge("?uuid1", "?uuid4", new LabeledEdge(4));
        expectedGraph4.addEdge("?uuid4", "?b", new LabeledEdge(2));

        // :name once and (:worksAt? / :foaf) once (:worksAt none)
        var expectedGraph5 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph5.addVertex("?a");
        expectedGraph5.addVertex("?b");
        expectedGraph5.addVertex("?uuid1");
        expectedGraph5.addVertex("?uuid2");
        expectedGraph5.addVertex("?uuid3");
        expectedGraph5.addEdge("?a", "?uuid2", new LabeledEdge(1));
        expectedGraph5.addEdge("?uuid1", "?uuid3", new LabeledEdge(3));
        expectedGraph5.addEdge("?uuid3", "?uuid2", new LabeledEdge(3));
        expectedGraph5.addEdge("?uuid1", "?b", new LabeledEdge(2));

        // :name once and (:worksAt? / :foaf) once (:worksAt once)
        var expectedGraph6 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph6.addVertex("?a");
        expectedGraph6.addVertex("?b");
        expectedGraph6.addVertex("?uuid1");
        expectedGraph6.addVertex("?uuid2");
        expectedGraph6.addVertex("?uuid3");
        expectedGraph6.addVertex("?uuid4");
        expectedGraph6.addEdge("?a", "?uuid2", new LabeledEdge(1));
        expectedGraph6.addEdge("?uuid1", "?uuid3", new LabeledEdge(3));
        expectedGraph6.addEdge("?uuid3", "?uuid2", new LabeledEdge(3));
        expectedGraph6.addEdge("?uuid1", "?uuid4", new LabeledEdge(4));
        expectedGraph6.addEdge("?uuid4", "?b", new LabeledEdge(2));

        assertThat(graphBuildingResult.getConstructedGraphs()).containsOnly(expectedGraph1, expectedGraph2, expectedGraph3, expectedGraph4, expectedGraph5, expectedGraph6);
        assertThat(graphBuildingResult.getEncounteredFeatures()).containsOnly(QueryFeature.PROPERTY_PATH.name(), PathFeature.ZERO_OR_ONE.name(), PathFeature.SEQ.name(), PathFeature.ONE_OR_MORE.name(), PathFeature.INVERSE.name());

    }

    @Test
    public void emptyQueryPatternTest() {
        var queryString = "DESCRIBE <http://example.org/>";

        var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
        var graphBuildingResult = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

        assertThat(graphBuildingResult.getConstructedGraphs()).isEmpty();
        assertThat(graphBuildingResult.getEncounteredFeatures()).isEmpty();
    }

    @Test
    public void serviceTest() {
        var queryString = PREFIX + "SELECT ?a WHERE { " +
                "?a :knows ?b . " +
                "SERVICE <http://people.example.org/sparql> {" +
                "   ?a :foaf ?c . " +
                "   ?a :name \"Karl\" " +
                "} }";

        var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
        var graphBuildingResult = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

        var expectedGraph1 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph1.addVertex("?a");
        expectedGraph1.addVertex("?b");

        expectedGraph1.addEdge("?a", "?b", new LabeledEdge(1));

        var expectedGraph2 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph2.addVertex("?a");
        expectedGraph2.addVertex("?c");
        expectedGraph2.addVertex("\"Karl\"");
        expectedGraph2.addEdge("?a", "?c", new LabeledEdge(2));
        expectedGraph2.addEdge("?a", "\"Karl\"", new LabeledEdge(3));

        assertThat(graphBuildingResult.getConstructedGraphs()).containsOnly(expectedGraph1, expectedGraph2);
        assertThat(graphBuildingResult.getEncounteredFeatures()).containsOnly(QueryFeature.SERVICE.name());
    }

    @Test
    public void namedGraphTest() {
        var queryString = PREFIX +
                "SELECT ?a " +
                "FROM NAMED <http://example.org/example> " +
                "WHERE { " +
                "?a :knows ?b . " +
                "GRAPH <http://example.org/example> {" +
                "   ?a :foaf ?c . " +
                "   ?a :name \"Karl\" " +
                "} }";

        var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
        var graphBuildingResult = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

        var expectedGraph1 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph1.addVertex("?a");
        expectedGraph1.addVertex("?b");

        expectedGraph1.addEdge("?a", "?b", new LabeledEdge(1));

        var expectedGraph2 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
        expectedGraph2.addVertex("?a");
        expectedGraph2.addVertex("?c");
        expectedGraph2.addVertex("\"Karl\"");
        expectedGraph2.addEdge("?a", "?c", new LabeledEdge(2));
        expectedGraph2.addEdge("?a", "\"Karl\"", new LabeledEdge(3));

        assertThat(graphBuildingResult.getConstructedGraphs()).containsOnly(expectedGraph1, expectedGraph2);
        assertThat(graphBuildingResult.getEncounteredFeatures()).containsOnly(QueryFeature.NAMED_GRAPH.name());
    }

    @Test
    public void variablePredicateTest() {
        var queryString = PREFIX + "SELECT ?a WHERE { " +
                "?a :knows ?b . " +
                "?a ?somePredicate ?c . " +
                "OPTIONAL { ?a :name \"Karl\" } " +
                " }";

        var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
        var graphBuildingResult = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

        assertThat(graphBuildingResult.getConstructedGraphs()).isEmpty();
        assertThat(graphBuildingResult.getEncounteredFeatures()).containsOnly(QueryFeature.VARIABLE_PREDICATE.name(), QueryFeature.OPTIONAL.name());
    }
}
