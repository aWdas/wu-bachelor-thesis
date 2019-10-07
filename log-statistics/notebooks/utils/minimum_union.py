import json
from multiprocessing import cpu_count

import pandas as pd

cores = cpu_count()


def import_statistics(file_path, to_set=False):
    lines = open(f"{file_path}.tsv").readlines()
    meta = import_meta(f"{file_path}_meta.tsv")
    counts = [(set(json.loads(left)) if to_set == True else json.loads(left), int(right)) for left, right in
              (line.strip().split("\t") for line in lines[1:])]
    if "EMPTY_GRAPH_PATTERN" not in meta.keys():
        empty_graph_pattern_weights = [entry[1] for entry in counts if len(entry[0]) == 0]
        meta["EMPTY_GRAPH_PATTERN"] = sum(empty_graph_pattern_weights)

    counts = [entry for entry in counts if len(entry[0]) > 0]
    return pd.DataFrame(counts, columns=["set", "weight"]), meta


def import_meta(file_path):
    lines = open(file_path).readlines()
    meta = {}
    for line in lines:
        split_line = line.strip().split("\t")
        meta[split_line[0]] = int(split_line[1])
    return meta


def import_combined(file_paths):
    dfs = []
    meta_combined = {}
    for path in file_paths:
        df, meta = import_statistics(path)
        meta_combined = {key: (meta_combined.get(key, 0) + meta.get(key, 0)) for key in set(meta_combined) | set(meta)}
        dfs.append(df)

    all_df = pd.concat(dfs)
    all_df.head()
    all_df["set"] = all_df["set"].map(json.dumps)
    all_df = all_df.groupby(all_df["set"]).sum().sort_values(by=['weight'], ascending=False).reset_index()
    all_df["set"] = all_df["set"].map(lambda s: set(json.loads(s)))
    return all_df, meta_combined


def plot_minimum_unions(minimum_unions, axes, title, x_interval=None, positions=None, max_cov=None):
    steps = [list(x) for x in zip(*[(len(mu["optimalPartitions"]), mu["weightSum"]) for mu in minimum_unions])]
    if x_interval is not None:
        axes.set_xlim(x_interval[0], x_interval[1])
    axes.set_ylim(0, 100)
    axes.minorticks_on()
    axes.grid(b=True, which="major", ls="-")
    axes.grid(b=True, which="minor", ls="--", lw=0.5)
    axes.set_title(title)
    axes.plot(steps[0], positions if positions is not None else steps[1], "b.-", markersize=4,
              label="Number of partitions for coverage level")
    if max_cov is not None:
        axes.axhline(max_cov, ls="--", c="r", lw=0.8, label="Maximum coverage")
    axes.set_xlabel('Number of partitions required')
    axes.set_ylabel('Percentage of queries covered')
    axes.legend()


def calc_coverage_progression(dfs, optimal_partitions):
    created_partitions = set()
    previous_periods_coverage = [None]
    same_period_coverage = [optimal_partitions[0]["weightSum"] / dfs[0][0]["weight"].sum()]
    for i in range(1, len(optimal_partitions)):
        df, meta = dfs[i]

        created_partitions |= set(optimal_partitions[i - 1]["optimalPartitions"])
        covered_weight_now = df.apply(lambda row: row["weight"] if row["set"].issubset(created_partitions) else 0,
                                      axis=1).sum()

        same_period_coverage.append(optimal_partitions[i]["weightSum"] / dfs[i][0]["weight"].sum())
        previous_periods_coverage.append(covered_weight_now / dfs[i][0]["weight"].sum())

    return [c * 100 for c in same_period_coverage], [c * 100 if c is not None else None for c in
                                                     previous_periods_coverage]


def plot_coverage_progression(same_period_coverage, previous_periods_coverage, axes, title):
    periods = list(range(1, len(same_period_coverage) + 1))
    axes.set_ylim(0, 100)
    axes.minorticks_on()
    axes.grid(b=True, which="major", ls="-")
    axes.grid(b=True, which="minor", ls="--", lw=0.5)
    axes.set_title(title)
    axes.plot(periods, same_period_coverage, "b.-", label="Coverage with this interval's partitions")
    axes.plot(periods, previous_periods_coverage, "r.-", label="Coverage with all previous intervals' partitions")
    axes.set_xlabel('Interval')
    axes.set_ylabel('Percentage of fully resolvable queries covered')
    axes.legend()
