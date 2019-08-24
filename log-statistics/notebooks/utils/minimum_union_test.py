import pandas as pd

import utils.minimum_union as mu


def test_minimum_union_same_set_size_no_duplicates():
    test_data = {"set": [{"1"}, {"2"}, {"3"}, {"4"}], "weight": [100, 200, 200, 150]}
    df = pd.DataFrame(data=test_data)

    result_list, weight_sum = mu.calc_minimum_union(df, 700)
    assert result_list == ["2", "3", "4", "1"]
    assert weight_sum == 650


def test_minimum_union_different_set_size_no_duplicates():
    test_data = {"set": [{"1"}, {"2", "3"}, {"3,5"}, {"4"}, {"6", "7"}], "weight": [100, 200, 200, 150, 100]}
    df = pd.DataFrame(data=test_data)

    result_list, weight_sum = mu.calc_minimum_union(df, 800)
    assert result_list == ["3,5", "4", "1", "2", "3", "6", "7"]
    assert weight_sum == 750


def test_minimum_union_different_set_size_with_duplicates():
    test_data = {
        "set": [{"1"}, {"2", "3,5"}, {"3,5"}, {"4", "2", "3,5"}, {"6", "7", "2"}, {"8", "9"}],
        "weight": [100, 200, 200, 150, 100, 100]
    }
    df = pd.DataFrame(data=test_data)

    result_list, weight_sum = mu.calc_minimum_union(df, 900)
    assert result_list == ["3,5", "2", "4", "1", "6", "7", "8", "9"]
    assert weight_sum == 850


def test_minimum_union_cheaper_threshold_crossing():
    test_data = {
        "set": [{"1"}, {"1", "2", "3"}, {"1", "4"}],
        "weight": [150, 200, 80]
    }
    df = pd.DataFrame(data=test_data)

    result_list, weight_sum = mu.calc_minimum_union(df, 200)
    assert result_list == ["1", "4"]
    assert weight_sum == 230

    result_list, weight_sum = mu.calc_minimum_union(df, 500)
    assert result_list == ["1", "2", "3", "4"]
    assert weight_sum == 430


def test_minimum_union_percent_different_set_size_with_duplicates():
    test_data = {
        "set": [{"1"}, {"2", "3,5"}, {"3,5"}, {"4", "2", "3,5"}, {"6", "7", "2"}, {"8", "9"}],
        "weight": [100, 200, 200, 150, 100, 100]
    }
    df = pd.DataFrame(data=test_data)

    result_list, weight_sum = mu.calc_minimum_union_percent(df, 1000, 90)
    assert result_list == ["3,5", "2", "4", "1", "6", "7", "8", "9"]
    assert weight_sum == 0.85


def test_minimum_unions():
    test_data = {
        "set": [{"1"}, {"2", "3,5"}, {"3,5"}, {"10", "11"}, {"4", "2", "3,5"}, {"6", "7", "2"}, {"8", "9"}, {"12"},
                {"2", "4"}],
        "weight": [100, 180, 200, 500, 130, 50, 50, 20, 20]
    }
    df = pd.DataFrame(data=test_data)

    result_list = mu.calc_minimum_unions(df, 1500, 10)
    assert len(result_list) == 9
    assert result_list[0][0] == ["3,5"]
    assert result_list[0][1] == 200 / 1500
    assert result_list[1][0] == ["10", "11"]
    assert result_list[1][1] == 500 / 1500
    assert result_list[2][0] == ["10", "11"]
    assert result_list[2][1] == 500 / 1500
    assert result_list[3][0] == ["10", "11", "3,5"]
    assert result_list[3][1] == 700 / 1500
    assert result_list[4][0] == ["10", "11", "3,5", "2"]
    assert result_list[4][1] == 880 / 1500
    assert result_list[5][0] == ["10", "11", "3,5", "2", "4"]
    assert result_list[5][1] == 1030 / 1500
    assert result_list[6][0] == ["10", "11", "3,5", "2", "4", "1"]
    assert result_list[6][1] == 1130 / 1500
    assert result_list[7][0] == ["10", "11", "3,5", "2", "4", "1", "6", "7", "12"]
    assert result_list[7][1] == 1200 / 1500
    assert result_list[8][0] == ["10", "11", "3,5", "2", "4", "1", "6", "7", "8", "9", "12"]
    assert result_list[8][1] == 1250 / 1500
