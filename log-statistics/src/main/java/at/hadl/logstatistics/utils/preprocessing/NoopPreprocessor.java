package at.hadl.logstatistics.utils.preprocessing;

import java.util.Optional;

public class NoopPreprocessor implements Preprocessor {
	@Override
	public Optional<String> extractQueryString(String logLine) {
		return Optional.of(logLine);
	}

	@Override
	public String preprocessQueryString(String queryString) {
		return queryString;
	}
}
