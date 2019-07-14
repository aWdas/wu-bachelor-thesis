package at.hadl.logstatistics.utils.preprocessing;

import java.util.Optional;

public class WikidataPreprocessor implements Preprocessor {

	@Override
	public Optional<String> extractQueryString(String logLine) {
		return decodeURLEncodedString(logLine.split("\t")[0]);
	}

	@Override
	public String preprocessQueryString(String queryString) {
		return queryString;
	}
}
