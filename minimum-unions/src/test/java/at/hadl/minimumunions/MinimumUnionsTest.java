package at.hadl.minimumunions;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class MinimumUnionsTest {

	@Test
	public void minimumUnionSameSetSizeNoDuplicatesTest() {
		var testData = Arrays.asList(
				WeightedSet.builder().set(Set.of("1")).weight(100).build(),
				WeightedSet.builder().set(Set.of("2")).weight(200).build(),
				WeightedSet.builder().set(Set.of("3")).weight(200).build(),
				WeightedSet.builder().set(Set.of("4")).weight(150).build()
		);

		var result = MinimumUnions.calculateMinimumUnion(testData, 700);

		assertThat(result.optimalPartitions).isEqualTo(Arrays.asList("2", "3", "4", "1"));
		assertThat(result.weightSum).isEqualTo(650);
	}

	@Test
	public void minimumUnionDifferentSetSizeNoDuplicatesTest() {
		var testData = Arrays.asList(
				WeightedSet.builder().set(Set.of("1")).weight(100).build(),
				WeightedSet.builder().set(Set.of("2", "3")).weight(200).build(),
				WeightedSet.builder().set(Set.of("3,5")).weight(200).build(),
				WeightedSet.builder().set(Set.of("4")).weight(150).build(),
				WeightedSet.builder().set(Set.of("6", "7")).weight(100).build()
		);

		var result = MinimumUnions.calculateMinimumUnion(testData, 800);

		assertThat(result.optimalPartitions).isEqualTo(Arrays.asList("3,5", "4", "1", "2", "3", "6", "7"));
		assertThat(result.weightSum).isEqualTo(750);
	}

	@Test
	public void minimumUnionDifferentSetSizeWithDuplicatesTest() {
		var testData = Arrays.asList(
				WeightedSet.builder().set(Set.of("1")).weight(100).build(),
				WeightedSet.builder().set(Set.of("2", "3,5")).weight(200).build(),
				WeightedSet.builder().set(Set.of("3,5")).weight(200).build(),
				WeightedSet.builder().set(Set.of("4", "2", "3,5")).weight(150).build(),
				WeightedSet.builder().set(Set.of("6", "7", "2")).weight(100).build(),
				WeightedSet.builder().set(Set.of("8", "9")).weight(100).build()
		);

		var result = MinimumUnions.calculateMinimumUnion(testData, 900);

		assertThat(result.optimalPartitions).isEqualTo(Arrays.asList("3,5", "2", "4", "1", "6", "7", "8", "9"));
		assertThat(result.weightSum).isEqualTo(850);
	}

	@Test
	public void minimumUnionCheaperThresholdCrossing() {
		var testData = Arrays.asList(
				WeightedSet.builder().set(Set.of("1")).weight(150).build(),
				WeightedSet.builder().set(Set.of("1", "2", "3")).weight(200).build(),
				WeightedSet.builder().set(Set.of("1", "4")).weight(80).build()
		);

		var result = MinimumUnions.calculateMinimumUnion(testData, 200);
		assertThat(result.optimalPartitions).isEqualTo(Arrays.asList("1", "4"));
		assertThat(result.weightSum).isEqualTo(230);

		result = MinimumUnions.calculateMinimumUnion(testData, 500);
		assertThat(result.optimalPartitions).isEqualTo(Arrays.asList("1", "2", "3", "4"));
		assertThat(result.weightSum).isEqualTo(430);
	}

	@Test
	public void minimumUnionOtherSetsIncluded() {
		var testData = Arrays.asList(
				WeightedSet.builder().set(Set.of("2")).weight(100).build(),
				WeightedSet.builder().set(Set.of("1", "2")).weight(200).build(),
				WeightedSet.builder().set(Set.of("3")).weight(140).build(),
				WeightedSet.builder().set(Set.of("3", "1")).weight(20).build(),
				WeightedSet.builder().set(Set.of("4")).weight(150).build()
		);

		var result = MinimumUnions.calculateMinimumUnion(testData, 500);
		assertThat(result.optimalPartitions).isEqualTo(Arrays.asList("4", "1", "2", "3"));
		assertThat(result.weightSum).isEqualTo(610);
	}

	@Test
	public void minimumUnionsTest() {
		var testData = Arrays.asList(
				WeightedSet.builder().set(Set.of("1")).weight(100).build(),
				WeightedSet.builder().set(Set.of("2", "3,5")).weight(160).build(),
				WeightedSet.builder().set(Set.of("3,5")).weight(180).build(),
				WeightedSet.builder().set(Set.of("10", "11")).weight(500).build(),
				WeightedSet.builder().set(Set.of("4", "2", "3,5")).weight(130).build(),
				WeightedSet.builder().set(Set.of("6", "7", "2")).weight(50).build(),
				WeightedSet.builder().set(Set.of("8", "9")).weight(50).build(),
				WeightedSet.builder().set(Set.of("12")).weight(20).build(),
				WeightedSet.builder().set(Set.of("2")).weight(40).build(),
				WeightedSet.builder().set(Set.of("2", "4")).weight(20).build()
		);

		var result = MinimumUnions.calculateMinimumUnions(testData, 1500, 10);

		assertThat(result.size()).isEqualTo(9);
		assertThat(result.get(0).optimalPartitions).isEqualTo(Arrays.asList("3,5"));
		assertThat(result.get(0).weightSum).isEqualTo(180);
		assertThat(result.get(1).optimalPartitions).isEqualTo(Arrays.asList("10", "11"));
		assertThat(result.get(1).weightSum).isEqualTo(500);
		assertThat(result.get(2).optimalPartitions).isEqualTo(Arrays.asList("10", "11"));
		assertThat(result.get(2).weightSum).isEqualTo(500);
		assertThat(result.get(3).optimalPartitions).isEqualTo(Arrays.asList("10", "11", "3,5"));
		assertThat(result.get(3).weightSum).isEqualTo(680);
		assertThat(result.get(4).optimalPartitions).isEqualTo(Arrays.asList("10", "11", "2", "3,5"));
		assertThat(result.get(4).weightSum).isEqualTo(880);
		assertThat(result.get(5).optimalPartitions).isEqualTo(Arrays.asList("10", "11", "2", "3,5", "4"));
		assertThat(result.get(5).weightSum).isEqualTo(1030);
		assertThat(result.get(6).optimalPartitions).isEqualTo(Arrays.asList("10", "11", "2", "3,5", "4", "1"));
		assertThat(result.get(6).weightSum).isEqualTo(1130);
		assertThat(result.get(7).optimalPartitions).isEqualTo(Arrays.asList("10", "11", "2", "3,5", "4", "1", "6", "7", "12"));
		assertThat(result.get(7).weightSum).isEqualTo(1200);
		assertThat(result.get(8).optimalPartitions).isEqualTo(Arrays.asList("10", "11", "2", "3,5", "4", "1", "6", "7", "8", "9", "12"));
		assertThat(result.get(8).weightSum).isEqualTo(1250);

	}
}