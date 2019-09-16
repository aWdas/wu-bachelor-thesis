package at.hadl.minimumunions;

import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MinimumUnions {
	public static MinimumUnionsResult calculateMinimumUnion(List<WeightedSet> weightedSets, Integer threshold) {
		var sets = weightedSets.stream().map(WeightedSet::new).collect(Collectors.toList());
		int weightSum = 0;
		var resultList = new ArrayList<String>();

		for(var weightedSet: sets) {
			weightedSet.setRemainingSet(weightedSet.getSet());
			weightedSet.setRelatedSets(sets.parallelStream()
					.filter(innerSet -> !Sets.intersection(innerSet.getSet(), weightedSet.getSet()).isEmpty() && !innerSet.equals(weightedSet))
					.collect(Collectors.toList()));
			weightedSet.setCumulativeWeight(weightedSet.getWeight() + weightedSet.getRelatedSets().stream()
					.filter(relatedSet -> Sets.difference(relatedSet.getSet(), weightedSet.getSet()).isEmpty())
					.mapToInt(WeightedSet::getWeight).sum());
			weightedSet.setRelativeWeight(weightedSet.getCumulativeWeight().doubleValue() / weightedSet.getRemainingSet().size());
		}

		while (weightSum < threshold) {
			if(sets.isEmpty()) {
				break;
			}

			WeightedSet topSet = sets.stream().min(Comparator.comparing(WeightedSet::getRelativeWeight, Comparator.reverseOrder())
					.thenComparing(w -> w.getRemainingSet().size())).orElseThrow();

			if(weightSum + topSet.getCumulativeWeight() >= threshold) {
				var remainingWeight = threshold - weightSum;
				topSet = sets.stream()
						.filter(weightedSet -> weightedSet.getCumulativeWeight() >= remainingWeight)
						.min(Comparator.comparing((WeightedSet w) -> w.getRemainingSet().size()).thenComparing(WeightedSet::getRelativeWeight, Comparator.reverseOrder())).orElseThrow();

			}

			resultList.addAll(topSet.getRemainingSet().stream().sorted().collect(Collectors.toList()));
			weightSum = weightSum + topSet.getCumulativeWeight();

			var topSetItems = new HashSet<>(topSet.getRemainingSet());

			sets = sets.stream()
					.peek(weightedSet -> weightedSet.setRemainingSet(Sets.difference(weightedSet.getRemainingSet(), topSetItems).immutableCopy()))
					.filter(weightedSet -> weightedSet.getRemainingSet().size() > 0)
					.collect(Collectors.toList());

			sets.parallelStream().forEach(weightedSet -> {
				/*weightedSet.setRelatedSets(weightedSet.getRelatedSets().stream()
						.filter(relatedSet -> relatedSet.getRemainingSet().size() > 0)
						.collect(Collectors.toList()));*/
				weightedSet.setCumulativeWeight(weightedSet.getWeight() + weightedSet.getRelatedSets().stream()
						.filter(relatedSet -> relatedSet.getRemainingSet().size() > 0 && Sets.difference(relatedSet.getRemainingSet(), weightedSet.getRemainingSet()).isEmpty())
						.mapToInt(WeightedSet::getWeight).sum());
				weightedSet.setRelativeWeight(weightedSet.getCumulativeWeight().doubleValue() / weightedSet.getRemainingSet().size());
			});
		}

		return MinimumUnionsResult.builder().optimalPartitions(resultList).weightSum(weightSum).threshold(threshold).build();
	}

	public static MinimumUnionsResult calculateMinimumUnionPercent(List<WeightedSet> weightedSets, Integer totalQueries, Double percentage) {
		if(percentage <= 0 || percentage > 100) {
			throw new RuntimeException();
		}

		var minimumUnion = calculateMinimumUnion(weightedSets, (int) Math.ceil(totalQueries / 100d * percentage));
		System.out.println("Minimum unions complete for percentage: " + percentage);

		return minimumUnion;
	}

	public static List<MinimumUnionsResult> calculateMinimumUnions(List<WeightedSet> weightedSets, Integer totalQueries) {
		return calculateMinimumUnions(weightedSets, totalQueries, 2);
	}

	public static List<MinimumUnionsResult> calculateMinimumUnions(List<WeightedSet> weightedSets, Integer totalQueries, int stepSize) {
		double maxCoverage = weightedSets.stream().mapToDouble(WeightedSet::getWeight).sum() / totalQueries * 100;
		List<Double> thresholdSteps = Stream.iterate((double) stepSize, i -> i <= maxCoverage + stepSize, i -> i + stepSize).collect(Collectors.toList());

		System.out.println("Max coverage: " + maxCoverage);
		System.out.println("Steps: " + thresholdSteps);

		return thresholdSteps.stream()
				.map(t -> calculateMinimumUnionPercent(weightedSets, totalQueries, t))
				.sorted(Comparator.comparing(MinimumUnionsResult::getThreshold))
				.collect(Collectors.toList());
	}
}
