from demographic_model import dataTransform_fertility, natModel
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def main():
    birth_data = pd.read_csv("births.csv").set_index("year").T
    female_pop_fert = pd.read_csv("fem_pop.csv").set_index("year").T
    birth_data_t = pd.read_csv("births_test.csv").set_index("year").T
    female_pop_fert_t = pd.read_csv("fem_pop_test.csv").set_index("year").T
    lmbda = 0.19

    original_tfr = dataTransform_fertility(female_pop_fert, birth_data, lmbda).tfr.reset_index(drop=True).T.reset_index(drop=True).T
    tfr_data = dataTransform_fertility(female_pop_fert, birth_data, lmbda).box_cox()
    tfr_t_data = dataTransform_fertility(female_pop_fert_t, birth_data_t, lmbda=lmbda).box_cox()

    nat_model = natModel(tfr_data, 6)
    nat_model_t = natModel(tfr_t_data, 6)


    error_list = pd.Series()
    j = 0
    for i in range(800, 1001):
        test_phi = i/1000
        beta_test = nat_model_t.project(5, phi=test_phi)
        test_forecast = nat_model_t.forecasted_component(lmbda, beta_test)
        error_frame = test_forecast.subtract(original_tfr, axis="columns")

        mse = np.pow(error_frame.stack().dropna().mean(), 2)

        error_list.loc[j] = mse
        j = j + 1

    selected_phi = (error_list.idxmin()+800)/1000
    print(f"optimal phi: {selected_phi}")

    beta_projection = nat_model.project(30, phi=selected_phi)
    nat_forecast = nat_model.forecasted_component(lmbda, beta_projection).T
    nat_forecast["year"] = nat_forecast.index + 1990

    nat_forecast.to_csv("tfr_forecast.csv")

    for i in range(0, 8):
        plt.plot(nat_forecast["year"], nat_forecast[i])
    plt.show()


if __name__ == "__main__":
    main()