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
		var queryGraphs = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

		var expectedGraph = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
		expectedGraph.addVertex("?a");
		expectedGraph.addVertex("?b");
		expectedGraph.addVertex("?c");
		expectedGraph.addVertex("\"Karl\"");

		expectedGraph.addEdge("?a", "?b", new LabeledEdge(1));
		expectedGraph.addEdge("?a", "?c", new LabeledEdge(2));
		expectedGraph.addEdge("?a", "\"Karl\"", new LabeledEdge(3));


		System.out.println(queryGraphs);

		assertThat(queryGraphs).contains(expectedGraph);
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
		var queryGraphs = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

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

		assertThat(queryGraphs).containsOnly(expectedGraph1, expectedGraph2);
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
		var queryGraphs = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

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

		assertThat(queryGraphs).containsOnly(expectedGraph1, expectedGraph2);
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
		var queryGraphs = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

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

		assertThat(queryGraphs).containsOnly(expectedGraph1, expectedGraph2);
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
		var queryGraphs = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

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

		assertThat(queryGraphs).containsOnly(expectedGraph1, expectedGraph2);
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
		var queryGraphs = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

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

		assertThat(queryGraphs).containsOnly(expectedGraph1, expectedGraph2);
	}

	@Test
	void orPropertyPathOnlyTest() {
		var queryString = PREFIX + "SELECT ?a WHERE { " +
				"?a :knows ?b . " +
				"?a :foaf | :worksAt ?c . " +
				" }";

		var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
		var queryGraphs = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

		var expectedGraph1 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
		expectedGraph1.addVertex("?a");
		expectedGraph1.addVertex("?b");
		expectedGraph1.addVertex("?c");
		expectedGraph1.addEdge("?a", "?b", new LabeledEdge(1));

		var expectedGraph2 = (DefaultDirectedGraph<String, LabeledEdge>) expectedGraph1.clone();

		expectedGraph1.addEdge("?a", "?c", new LabeledEdge(2));
		expectedGraph2.addEdge("?a", "?c", new LabeledEdge(4));

		assertThat(queryGraphs).containsOnly(expectedGraph1, expectedGraph2);
	}

	@Test
	void orAndSequencePropertyPathTest() {
		var queryString = PREFIX + "SELECT ?a WHERE { " +
				"?a :knows ?b . " +
				"?a :foaf / :name | :worksAt ?c . " +
				" }";

		var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
		var queryGraphs = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

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

		assertThat(queryGraphs).containsOnly(expectedGraph1, expectedGraph2);
	}

	@Test
	void zeroOrMorePropertyPathOnlyTest() {
		var queryString = PREFIX + "SELECT ?a WHERE { " +
				"?a :knows ?b . " +
				"?a :foaf* ?c . " +
				" }";

		var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
		var queryGraphs = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

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

		assertThat(queryGraphs).containsOnly(expectedGraph1, expectedGraph2, expectedGraph3);
	}

	@Test
	void oneOrMorePropertyPathOnlyTest() {
		var queryString = PREFIX + "SELECT ?a WHERE { " +
				"?a :knows ?b . " +
				"?a :foaf+ ?c . " +
				" }";

		var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
		var queryGraphs = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

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

		assertThat(queryGraphs).containsOnly(expectedGraph1, expectedGraph2);
	}

	@Test
	void zeroOrOnePropertyPathOnlyTest() {
		var queryString = PREFIX + "SELECT ?a WHERE { " +
				"?a :knows ?b . " +
				"?a :foaf? ?c . " +
				" }";

		var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
		var queryGraphs = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

		var expectedGraph1 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
		expectedGraph1.addVertex("?a");
		expectedGraph1.addVertex("?b");
		expectedGraph1.addEdge("?a", "?b", new LabeledEdge(1));

		var expectedGraph2 = (DefaultDirectedGraph<String, LabeledEdge>) expectedGraph1.clone();

		expectedGraph2.addVertex("?c");
		expectedGraph2.addEdge("?a", "?c", new LabeledEdge(2));

		assertThat(queryGraphs).containsOnly(expectedGraph1, expectedGraph2);
	}

	@Test
	void negationPropertyPathOnlyTest() {
		var queryString = PREFIX + "SELECT ?a WHERE { " +
				"?a :knows ?b . " +
				"?a !:foaf ?c . " +
				" }";

		var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
		var queryGraphs = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

		assertThat(queryGraphs).isEqualTo(Collections.emptyList());
	}

	@Test
	void inversePropertyPathOnlyTest() {
		var queryString = PREFIX + "SELECT ?a WHERE { " +
				"?a :knows ?b . " +
				"?a ^:foaf ?c . " +
				" }";

		var query = QueryFactory.create(queryString, Syntax.syntaxSPARQL_11);
		var queryGraphs = graphBuilder.constructGraphsFromQuery(query, PREDICATE_MAP);

		var expectedGraph1 = new DefaultDirectedGraph<String, LabeledEdge>(LabeledEdge.class);
		expectedGraph1.addVertex("?a");
		expectedGraph1.addVertex("?b");
		expectedGraph1.addVertex("?c");
		expectedGraph1.addEdge("?a", "?b", new LabeledEdge(1));
		expectedGraph1.addEdge("?c", "?a", new LabeledEdge(2));

		assertThat(queryGraphs).containsOnly(expectedGraph1);
	}
}
