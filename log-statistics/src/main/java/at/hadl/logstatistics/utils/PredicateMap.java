package at.hadl.logstatistics.utils;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class PredicateMap {
	private ConcurrentHashMap<String, Integer> predicateMap;
	private ConcurrentHashMap<Integer, String> reversePredicateMap;
	private AtomicInteger predicateMappingSequence;

	public PredicateMap() {
		this.predicateMap = new ConcurrentHashMap<>(2500);
		this.reversePredicateMap = new ConcurrentHashMap<>(2500);
		this.predicateMappingSequence = new AtomicInteger(0);
	}

	public PredicateMap(ConcurrentHashMap<String, Integer> predicateMap) {
		this.predicateMap = predicateMap;
		this.predicateMappingSequence = new AtomicInteger(Collections.max(predicateMap.values()));
		this.reversePredicateMap = new ConcurrentHashMap<>();
		predicateMap.forEach((key, value) -> reversePredicateMap.put(value, key));
	}

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

	public ConcurrentHashMap<String, Integer> getPredicateMap() {
		return predicateMap;
	}

	public void setPredicateMap(ConcurrentHashMap<String, Integer> predicateMap) {

	}

	public ConcurrentHashMap<Integer, String> getReversePredicateMap() {
		return reversePredicateMap;
	}
}
