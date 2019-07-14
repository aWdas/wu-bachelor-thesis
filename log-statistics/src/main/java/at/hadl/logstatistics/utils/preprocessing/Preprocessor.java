package at.hadl.logstatistics.utils.preprocessing;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public interface Preprocessor {
	Optional<String> extractQueryString(String logLine);

	String preprocessQueryString(String queryString);

	default Optional<String> decodeURLEncodedString(String encodedString) {
		try {
			return Optional.of(URLDecoder.decode(encodedString, StandardCharsets.UTF_8));
		} catch (Exception e) {
			return Optional.empty();
		}
	}
}
