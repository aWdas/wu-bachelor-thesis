package at.hadl.minimumunions;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Application {
	/*
	Calculate the optimal partitions for 1 or more datasets
	Limit: How many pairs (S_i, w_i) to include in the calculation (Sorted by w_i).
		   If fewer than all are chosen, the maximum coverage calculated by the application is slightly below
		   the actual one (as there would be many tiny partitions left to include),
		   but the results for all coverage steps levels before the highest are unaffected
	 Datasets: the names of the result files returned by the "log-statistics" application, but without the .tsv ending
	 		   the .tsv and _meta.tsv files are assumed to lie in the same directory
	 PathPrefix: The path from which to load the query shape analysis results
	 */
	public static void main(String[] args) {
		var datasets = new ArrayList<String>();
		IntStream.range(1, 8).forEach(i -> datasets.add("wikidata_all_" + i));

		processIndividually(datasets, "../../results/queryshapes/", 10000);
	}

	// Calculate the optimal partitions for each dataset individually -> one output file per dataset
	public static void processIndividually(List<String> datasets, String pathPrefix, int limit) {
		datasets.forEach(dataset -> {
			System.out.println("Processing " + dataset + ".tsv");
			var weightedSets = WeightedSetLoader.loadWeightedSets(
					Collections.singletonList(Path.of(pathPrefix + dataset + ".tsv")),
					limit);

			var meta = WeightedSetLoader.loadMeta(
					Collections.singletonList(Path.of(pathPrefix + dataset + "_meta.tsv")));

			var results = MinimumUnions.calculateMinimumUnions(weightedSets, meta.get("VALID_QUERIES"));

			try (var fileWriter = new FileWriter(dataset + "_minimum_unions.json")) {
				fileWriter.write(new ObjectMapper().writeValueAsString(results));
			} catch (IOException e) {
				throw new RuntimeException();
			}
		});
	}

	// Calculate the optimal partitions for all datasets combined -> one combined output file
	public static void processTogether(List<String> datasets, String outputName, String pathPrefix, int limit) {
		var weightedSets = WeightedSetLoader.loadWeightedSets(
				datasets.stream().map(dataset -> Path.of(pathPrefix + dataset + ".tsv")).collect(Collectors.toList()),
				limit);

			var meta = WeightedSetLoader.loadMeta(
					datasets.stream().map(dataset -> Path.of(pathPrefix + dataset + "_meta.tsv")).collect(Collectors.toList()));

			var results = MinimumUnions.calculateMinimumUnions(weightedSets, meta.get("VALID_QUERIES"));

		try (var fileWriter = new FileWriter(outputName + "_minimum_unions.json")) {
				fileWriter.write(new ObjectMapper().writeValueAsString(results));
			} catch (IOException e) {
				throw new RuntimeException();
		}
	}
}
