package at.hadl.minimumunions;

import com.google.common.collect.Sets;
import org.javatuples.Pair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

public class MinimumUnions {
	public static MinimumUnionsResult calculateMinimumUnion(List<WeightedSet> weightedSets, Integer threshold) {
		var sets = weightedSets.stream().map(WeightedSet::new).collect(Collectors.toList());
		var weightSum = 0;
		var resultList = new ArrayList<String>();

		for(var weightedSet: sets) {
			weightedSet.setRemainingSet(weightedSet.getSet());
			weightedSet.setCumulativeWeight(sets.parallelStream()
					.filter(innerSet -> Sets.difference(innerSet.getSet(), weightedSet.getSet()).isEmpty())
					.mapToInt(WeightedSet::getWeight).sum());
			weightedSet.setRelativeWeight(weightedSet.getCumulativeWeight().doubleValue() / weightedSet.getRemainingSet().size());
		}

		while (weightSum < threshold) {
			if(sets.isEmpty()) {
				break;
			}

			WeightedSet topSet = sets.stream()
					.sorted(Comparator.comparing(WeightedSet::getRelativeWeight, Comparator.reverseOrder())
							.thenComparing(w -> w.getRemainingSet().size())).findFirst().orElseThrow();

			if(weightSum + topSet.getCumulativeWeight() >= threshold) {
				var remainingWeight = threshold - weightSum;
				topSet = sets.stream()
						.filter(weightedSet -> weightedSet.getCumulativeWeight() >= remainingWeight)
						.sorted(Comparator.comparing((WeightedSet w) -> w.getRemainingSet().size()).thenComparing(WeightedSet::getRelativeWeight, Comparator.reverseOrder()))
						.findFirst().orElseThrow();

			}

			resultList.addAll(topSet.getRemainingSet().stream().sorted().collect(Collectors.toList()));
			weightSum = weightSum + topSet.getCumulativeWeight();

			var topSetItems = topSet.getRemainingSet();

			sets = sets.stream()
					.peek(weightedSet -> weightedSet.setRemainingSet(Sets.difference(weightedSet.getRemainingSet(), topSetItems).immutableCopy()))
					.filter(weightedSet -> weightedSet.getRemainingSet().size() > 0)
					.collect(Collectors.toList());

			var setsCopy = sets.parallelStream()
					.map(weightedSet -> new Pair<>(weightedSet.getRemainingSet(), weightedSet.getWeight()))
					.collect(Collectors.toList());

			sets.parallelStream().forEach(weightedSet -> {
				weightedSet.setCumulativeWeight(setsCopy.parallelStream()
						.filter(innerSet -> Sets.difference(innerSet.getValue0(), weightedSet.getRemainingSet()).isEmpty())
						.mapToInt(Pair::getValue1).sum());
				weightedSet.setRelativeWeight(weightedSet.getCumulativeWeight().doubleValue() / weightedSet.getRemainingSet().size());
			});
		}

		return MinimumUnionsResult.builder().optimalPartitions(resultList).weightSum(weightSum).build();
	}

	public static MinimumUnionsResult calculateMinimumUnionPercent(List<WeightedSet> weightedSets, Integer totalQueries, Double percentage) {
		if(percentage <= 0 || percentage > 100) {
			throw new RuntimeException();
		}

		System.out.println("Minimum unions complete for percentage: " + percentage);

		return calculateMinimumUnion(weightedSets, (int) Math.ceil(totalQueries / 100 * percentage));
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
				.collect(Collectors.toList());
	}
}
