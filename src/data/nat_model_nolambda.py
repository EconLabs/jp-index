from demographic_model import dataTransform_fertility, natModel
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm


def main():
    birth_data = pd.read_csv("births.csv").set_index("year").T
    female_pop_fert = pd.read_csv("fem_pop.csv").set_index("year").T
    birth_data_t = pd.read_csv("births_test.csv").set_index("year").T
    female_pop_fert_t = pd.read_csv("fem_pop_test.csv").set_index("year").T

    original_tfr = dataTransform_fertility(female_pop_fert, birth_data, 0).tfr.reset_index(drop=True).T.reset_index(drop=True).T
    
    error_phi = pd.DataFrame()
    error_lambda = pd.Series()
    for i in tqdm(range(1, 101)):
        test_l = i/100
        tfr_t_data = dataTransform_fertility(female_pop_fert_t, birth_data_t, lmbda=test_l).smooth_data()
        model_test = natModel(tfr_t_data, 6)
        j = 0
        for k in range(800, 1001):
            test_phi = k/1000
            beta_test = model_test.project(5, phi=test_phi)
            test_forecast = model_test.forecasted_component(test_l, beta_test)
            error_frame = test_forecast.subtract(original_tfr, axis="columns")

            mse = np.pow(error_frame.stack().dropna().mean(), 2)

            error_phi.at[j, i] = mse
            j = j + 1
        error_lambda[i] = error_phi[i].mean()

    selected_lambda = (error_lambda.idxmin()+1)/100
    selected_phi = (error_phi[selected_lambda*100].idxmin()+800)/1000
    print(f"optimal_lambda: {selected_lambda}")
    print(f"optimal phi: {selected_phi}")

    tfr_data = dataTransform_fertility(female_pop_fert, birth_data, selected_lambda).smooth_data()
    nat_model = natModel(tfr_data, 6)
    beta_projection = nat_model.project(30, phi=selected_phi)
    nat_forecast = nat_model.forecasted_component(selected_lambda, beta_projection).T
    nat_forecast["year"] = nat_forecast.index + 1990

    nat_forecast.to_csv("tfr_forecast.csv")

    for i in range(0, 8):
        plt.plot(nat_forecast["year"], nat_forecast[i])
    plt.show()


if __name__ == "__main__":
    main()