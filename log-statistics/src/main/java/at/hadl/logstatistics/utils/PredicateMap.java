package at.hadl.logstatistics.utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class PredicateMap {
	private ConcurrentHashMap<String, Integer> predicateMap = new ConcurrentHashMap<>(10000);
	private ConcurrentHashMap<Integer, String> reversePredicateMap = new ConcurrentHashMap<>(10000);
	private AtomicInteger predicateMappingSequence = new AtomicInteger(0);

	public int getIntForPredicate(String predicate) {
		var predicateMapping = predicateMap.get(predicate);

		if (predicateMapping != null) {
			return predicateMapping;
		} else {
			var nextMapping = predicateMappingSequence.incrementAndGet();
			predicateMap.put(predicate, nextMapping);
			reversePredicateMap.put(nextMapping, predicate);
			return nextMapping;
		}
	}

	public String getPredicateForInt(int mappingNumber) {
		return reversePredicateMap.get(mappingNumber);
	}

	public int size() {
		return predicateMap.size();
	}
}
