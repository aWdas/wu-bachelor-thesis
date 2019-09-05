package at.hadl.minimumunions;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class WeightedSetLoader {
	public static List<WeightedSet> loadWeightedSets(List<Path> paths, Integer limit) {
		var combinedEntries = paths.stream()
				.flatMap(path -> {
					try {
						return Files.lines(path).skip(1);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				})
				.filter(line -> !line.isEmpty())
				.map(line -> {
					var parts = line.split("\t");
					return Map.entry(parts[0], Integer.parseInt(parts[1]));
				})
				.collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.summingInt(Map.Entry::getValue)));

		ObjectMapper objectMapper = new ObjectMapper();
		return combinedEntries.entrySet().stream()
				.map(entry -> {
					try {
						return WeightedSet.builder()
								.set(Set.of(objectMapper.readValue(entry.getKey(), String[].class)))
								.weight(entry.getValue())
								.build();
					} catch (IOException e) {
						throw new RuntimeException();
					}
				})
				.sorted(Comparator.comparing(WeightedSet::getWeight).reversed())
				.limit(limit)
				.collect(Collectors.toList());
	}

	public static Map<String, Integer> loadMeta(List<Path> paths) {
		return paths.stream()
				.flatMap(path -> {
					try {
						return Files.lines(path);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				})
				.filter(line -> !line.isEmpty())
				.map(line -> {
					var parts = line.split("\t");
					return Map.entry(parts[0], Integer.parseInt(parts[1]));
				})
				.collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.summingInt(Map.Entry::getValue)));
	}
}
