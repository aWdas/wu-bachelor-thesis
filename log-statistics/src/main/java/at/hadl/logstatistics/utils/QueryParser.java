package at.hadl.logstatistics.utils;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryException;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;

import java.util.Optional;

public class QueryParser {
	public static Optional<Query> parseQuery(String queryString) {
		try {
			return Optional.of(QueryFactory.create(queryString, Syntax.defaultSyntax));
		} catch (QueryException e) {
			return Optional.empty();
		}
	}

	public static Query parseQueryFailing(String queryString) {
		return QueryFactory.create(queryString, Syntax.defaultSyntax);
	}
}
