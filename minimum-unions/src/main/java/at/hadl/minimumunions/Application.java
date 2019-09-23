package at.hadl.minimumunions;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.openjdk.jmh.runner.RunnerException;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;

public class Application {
	public static void main(String[] args) throws IOException, RunnerException {
//		var prefix = "wikidata_organic_";
		var datasets = new ArrayList<String>();
		datasets.add("wikidata_organic_3");
		datasets.add("wikidata_organic_7");
		/*for (var i = 4; i <= 7; i++) {
			datasets.add(prefix + i);
		}*/

		datasets.forEach(dataset -> {
			System.out.println("Processing " + dataset + ".tsv");
			var weightedSets = WeightedSetLoader.loadWeightedSets(
					Collections.singletonList(Path.of("new-queryshapes-results/" + dataset + ".tsv")),
					40000);

			var meta = WeightedSetLoader.loadMeta(
					Collections.singletonList(Path.of("new-queryshapes-results/" + dataset + "_meta.tsv")));

			var results = MinimumUnions.calculateMinimumUnions(weightedSets, meta.get("VALID_QUERIES"));

			try (var fileWriter = new FileWriter(dataset + "_minimum_unions.json")) {
				fileWriter.write(new ObjectMapper().writeValueAsString(results));
			} catch (IOException e) {
				throw new RuntimeException();
			}
		});

		/*var weightedSets = WeightedSetLoader.loadWeightedSets(
				datasets.stream().map(dataset -> Path.of("new-queryshapes-results/" + dataset + ".tsv")).collect(Collectors.toList()),
				25000);

			var meta = WeightedSetLoader.loadMeta(
					datasets.stream().map(dataset -> Path.of("new-queryshapes-results/" + dataset + "_meta.tsv")).collect(Collectors.toList()));

			var results = MinimumUnions.calculateMinimumUnions(weightedSets, meta.get("VALID_QUERIES"));

		try (var fileWriter = new FileWriter(prefix + "minimum_unions.json")) {
				fileWriter.write(new ObjectMapper().writeValueAsString(results));
			} catch (IOException e) {
				throw new RuntimeException();
			}*/
	}

	/*@Benchmark
	@BenchmarkMode(Mode.AverageTime)
	@Fork(value = 1)
	@Warmup(iterations = 0)
	@Measurement(iterations = 3)
	public void benchmark() {
		var weightedSets = WeightedSetLoader.loadWeightedSets(Arrays.asList(
				Path.of("new-queryshapes-results/usewod_2013.tsv"),
				Path.of("new-queryshapes-results/usewod_2014.tsv"),
				Path.of("new-queryshapes-results/usewod_2015.tsv"),
				Path.of("new-queryshapes-results/usewod_2016.tsv")
		), 40000);

		var combinedMeta = WeightedSetLoader.loadMeta(Arrays.asList(
				Path.of("new-queryshapes-results/usewod_2013_meta.tsv"),
				Path.of("new-queryshapes-results/usewod_2014_meta.tsv"),
				Path.of("new-queryshapes-results/usewod_2015_meta.tsv"),
				Path.of("new-queryshapes-results/usewod_2016_meta.tsv")
		));

		var results = MinimumUnions.calculateMinimumUnionPercent(weightedSets, combinedMeta.get("VALID_QUERIES"), 50d);
		System.out.println(results);
	}*/
}
