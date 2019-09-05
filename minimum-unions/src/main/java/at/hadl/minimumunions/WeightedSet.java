package at.hadl.minimumunions;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.Set;

@Builder
@Data
@AllArgsConstructor
public class WeightedSet {
	private Set<String> set;
	private Integer weight;
	private Set<String> remainingSet;
	private Integer cumulativeWeight;
	private Double relativeWeight;

	public WeightedSet(WeightedSet weightedSet) {
		this.set = weightedSet.getSet();
		this.weight = weightedSet.getWeight();
	}
}
