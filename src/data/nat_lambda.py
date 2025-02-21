from demographic_model import dataTransform_fertility, natModel
import pandas as pd
import numpy as np
from scipy.special import inv_boxcox


def main():
    nat_data_test = pd.read_csv("births_test.csv").set_index("year").T
    female_pop_fert_test = pd.read_csv("fem_pop_test.csv").set_index("year").T
    nat_data = pd.read_csv("births.csv").set_index("year").T
    female_pop_fert = pd.read_csv("fem_pop.csv").set_index("year").T

    j = 0
    
    error_list = pd.Series()
    lambda_list = pd.Series()
    for i in range(1,101):
        l = i/100
        transformed = dataTransform_fertility(female_pop_fert_test, nat_data_test, lmbda = l)
        transformed_og = dataTransform_fertility(female_pop_fert, nat_data, lmbda=0)
        original_tfr = transformed_og.tfr.reset_index(drop=True).T.reset_index(drop=True).T
        nat_model = natModel(transformed.smooth_data(), 6)
        projected_b = nat_model.project(5)
        projected_data = nat_model.forecasted_component(l, projected_b)

        error_frame = projected_data.subtract(original_tfr, axis="columns")

        mean_error = np.pow(error_frame.stack().dropna().mean(), 2)

        error_list.loc[j] = mean_error
        lambda_list.loc[j] = i
        j = j + 1

    print(error_list)
    min_error_index = error_list.idxmin()
    min_lambda = lambda_list[min_error_index]
    print(error_list.iloc[min_error_index])
    print(min_lambda)


if __name__ == "__main__":
    main()