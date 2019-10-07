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
		var datasets = new ArrayList<String>();
		datasets.add("queryshapes_samples");

		datasets.forEach(dataset -> {
			System.out.println("Processing " + dataset + ".tsv");
			var weightedSets = WeightedSetLoader.loadWeightedSets(
					Collections.singletonList(Path.of("new-queryshapes-results/" + dataset + ".tsv")),
					10000);

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
}
