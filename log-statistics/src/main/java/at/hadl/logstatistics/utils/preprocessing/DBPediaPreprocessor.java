package at.hadl.logstatistics.utils.preprocessing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DBPediaPreprocessor implements Preprocessor {
	private static String defaultPrefixes;

	public DBPediaPreprocessor() {
		try {
			defaultPrefixes = getDefaultPrefixes();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static String addDefaultPrefixes(String queryString) {
		return defaultPrefixes + "\n" + queryString;
	}

	public static String removeIncorrectCommas(String s) {
		Pattern p = Pattern.compile("(select|SELECT).*?(where|WHERE)", Pattern.DOTALL);
		Matcher m = p.matcher(s);
		if (m.find()) {
			var selectClause = m.group();

			return s.replace(selectClause, selectClause.replaceAll("(\\?[a-zA-Z0-9_]*?) ?,", "$1"));
		} else {
			return s;
		}
	}

	public static String removeVirtuosoPragmas(String s) {
		return s.replaceAll("(define|DEFINE) .*?:.*? \".*?\"", "");
	}

	private static String getDefaultPrefixes() throws IOException {
		String defaultPrefixes;

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(ClassLoader.getSystemResourceAsStream("prefix-mappings-reduced.txt")))) {
			defaultPrefixes = reader.lines()
					.map(prefixLine -> {
						var prefixLineParts = prefixLine.split("=");
						return "PREFIX " + prefixLineParts[0] + ":<" + prefixLineParts[1] + ">";
					})
					.collect(Collectors.joining("\n"));
		}

		return defaultPrefixes;
	}

	public String preprocessQueryString(String queryString) {
		return Stream.of(queryString)
				.map(DBPediaPreprocessor::addDefaultPrefixes)
				.map(DBPediaPreprocessor::removeVirtuosoPragmas)
				.map(DBPediaPreprocessor::removeIncorrectCommas)
				.findAny()
				.orElseThrow();
	}

	public Optional<String> extractQueryString(String logLine) {
		Pattern regex = Pattern.compile("query=(.*?)(&| HTTP|\")");

		Matcher m = regex.matcher(logLine);

		if (m.find()) {
			String queryString = m.group(1);
			return decodeURLEncodedString(queryString);
		} else {
			return Optional.empty();
		}
	}
}
