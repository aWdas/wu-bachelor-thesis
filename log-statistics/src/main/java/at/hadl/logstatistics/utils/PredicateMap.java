package at.hadl.logstatistics.utils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class PredicateMap {
	private ConcurrentHashMap<String, Integer> predicateMap;
	private AtomicInteger predicateMappingSequence;

	public PredicateMap() {
		this.predicateMap = new ConcurrentHashMap<>(20000);
		this.predicateMappingSequence = new AtomicInteger(0);
	}

	public PredicateMap(ConcurrentHashMap<String, Integer> predicateMap) {
		this.predicateMap = predicateMap;
		this.predicateMappingSequence = new AtomicInteger(Collections.max(predicateMap.values()));
	}

	public static Optional<PredicateMap> fromPath(Path path) {
		try {
			ConcurrentHashMap<String, Integer> predicateHashMap = new ConcurrentHashMap<>();
			Files.lines(path).forEach(line -> {
				var lineParts = line.split("\t");
				predicateHashMap.put(lineParts[0], Integer.parseInt(lineParts[1]));
			});
			return Optional.of(new PredicateMap(predicateHashMap));
		} catch (Exception e) {
			e.printStackTrace();
			return Optional.empty();
		}
	}

	public int getIntForPredicate(String predicate) {
		var predicateMapping = predicateMap.get(predicate);

		if (predicateMapping != null) {
			return predicateMapping;
		} else {
			predicateMap.computeIfAbsent(predicate, (key) -> predicateMappingSequence.incrementAndGet());
			return predicateMap.get(predicate);
		}
	}

	public int size() {
		return predicateMap.size();
	}

	public ConcurrentHashMap<String, Integer> getPredicateMap() {
		return predicateMap;
	}
}
