import json
from multiprocessing import cpu_count, Pool

import math
import pandas as pd
import time

cores = cpu_count()


def import_statistics(file_path, to_set=False):
    lines = open(f"{file_path}.tsv").readlines()
    meta = import_meta(f"{file_path}_meta.tsv")
    counts = [(set(json.loads(left)) if to_set == True else json.loads(left), int(right)) for left, right in
              (line.strip().split("\t") for line in lines[1:])]
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


def calc_minimum_union(df, threshold):
    result_set = set()
    result_list = []
    weight_sum = 0
    df = df.copy()
    df["remaining"] = df["set"]
    df["len_remaining"] = df["remaining"].map(len)
    df["cumulative_weight"] = df.apply(lambda row: calc_cumulative_weight(row, df), axis=1)
    df["relative_weight"] = df["cumulative_weight"] / df["len_remaining"]

    while weight_sum < threshold:
        if df.shape[0] == 0:
            break

        start = time.time()
        top_row = df.sort_values(["relative_weight", "len_remaining"], ascending=[False, True]).iloc[0]
        print(time.time() - start)

        if weight_sum + top_row["weight"] >= threshold:
            threshold_crossing_rows = df[df["cumulative_weight"] >= (threshold - weight_sum)]
            top_row = threshold_crossing_rows.sort_values(["len_remaining", "relative_weight"], ascending=[True, False]).iloc[0]

        new_result_items = top_row["remaining"]
        result_set |= new_result_items
        result_list.extend(sorted(new_result_items))
        print(time.time() - start)
        df, weight_sum = add_all_fully_covered_rows(df, weight_sum, new_result_items)
        print(time.time() - start)
        df["remaining"] = df["remaining"].map(lambda s: s - new_result_items)
        print(time.time() - start)

        if df.shape[0] != 0:
            df["len_remaining"] = df["remaining"].map(len)
            print(time.time() - start)

            df["cumulative_weight"] = df.apply(lambda row: calc_cumulative_weight(row, df), axis=1)
            print(time.time() - start)

            df["relative_weight"] = df["cumulative_weight"] / df["len_remaining"]
            print(time.time() - start)

    return result_list, weight_sum


def calc_cumulative_weight(row, df):
    covered_rows = df["remaining"].map(lambda remaining: len(remaining - row["remaining"]) == 0)
    return df[covered_rows]["weight"].sum()


def add_all_fully_covered_rows(df, weight_sum, new_result_items):
    fully_covered = df["remaining"].map(lambda remaining: len(remaining - new_result_items) == 0)
    weight_sum = weight_sum + df[fully_covered]["weight"].sum()
    df = df[~fully_covered]
    return df, weight_sum


def calc_minimum_unions(df, total_queries, step_size=2):
    max_cov = df["weight"].sum() / total_queries * 100
    pool = Pool(cores)
    thresholds = range(step_size, (math.ceil(max_cov / step_size) + 1) * step_size, step_size)
    print(max_cov)
    print(list(thresholds))

    minimum_unions = pool.starmap(calc_minimum_union_percent, [(df, total_queries, thres) for thres in thresholds])
    return minimum_unions


def calc_minimum_union_percent(df, total_queries, threshold):
    threshold_value = total_queries * threshold / 100
    result_list, weight_sum = calc_minimum_union(df, threshold_value)
    return result_list, (weight_sum / total_queries)


def plot_minimum_unions(minimum_unions, axes, title, max_x=None, positions=None, max_cov=None):
    steps = [list(x) for x in zip(*[(len(result_set), weight_sum) for result_set, weight_sum in minimum_unions])]
    if max_x is not None:
        axes.set_xlim(-100, max_x)
    axes.set_ylim(0, 100)
    axes.minorticks_on()
    axes.grid(b=True, which="major", ls="-")
    axes.grid(b=True, which="minor", ls="--", lw=0.5)
    axes.set_title(title)
    axes.plot(steps[0], positions if positions is not None else steps[1], "b.-")
    if max_cov is not None:
        axes.axhline(max_cov, ls="--", c="r", lw=0.8)
    axes.set_xlabel('partitions')
    axes.set_ylabel('percent of queries')


def calc_coverage_progression(dfs, optimal_partitions):
    created_partitions = set()
    previous_periods_coverage = [None]
    same_period_coverage = [optimal_partitions[0][1]]
    for i in range(1, len(optimal_partitions)):
        df, meta = dfs[i]

        created_partitions |= set(optimal_partitions[i - 1][0])
        covered_weight_now = df.apply(lambda row: row["weight"] if row["set"].issubset(created_partitions) else 0,
                                      axis=1).sum()

        same_period_coverage.append(optimal_partitions[i][1])
        previous_periods_coverage.append(covered_weight_now / meta["VALID_QUERIES"])

    return same_period_coverage, previous_periods_coverage


def plot_coverage_progression(same_period_coverage, previous_periods_coverage, axes, title):
    periods = list(range(1, len(same_period_coverage) + 1))
    axes.set_ylim(0, 1)
    axes.minorticks_on()
    axes.grid(b=True, which="major", ls="-")
    axes.grid(b=True, which="minor", ls="--", lw=0.5)
    axes.set_title(title)
    axes.plot(periods, same_period_coverage, "b.-", label="this period's partitions")
    axes.plot(periods, previous_periods_coverage, "r.-", label="all previous periods' partitions")
    axes.set_xlabel('period')
    axes.set_ylabel('percent of queries')
    axes.legend()
