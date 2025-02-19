from demographic_model import dataTransform_mortality, mortModel
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def main():
    male_pop = pd.read_csv("exposure_male.csv").set_index("year").T
    male_deaths = pd.read_csv("deaths_male.csv").set_index("year").T
    female_pop = pd.read_csv("exposure_female.csv").set_index("year").T
    female_deaths = pd.read_csv("deaths_female.csv").set_index("year").T
    
    male_pop_t = pd.read_csv("exposure_male_test.csv").set_index("year").T
    male_deaths_t = pd.read_csv("deaths_male_test.csv").set_index("year").T
    female_pop_t = pd.read_csv("exposure_female_test.csv").set_index("year").T
    female_deaths_t = pd.read_csv("deaths_female_test.csv").set_index("year").T

    male_data = dataTransform_mortality(male_pop, male_deaths).smooth_data()
    female_data = dataTransform_mortality(female_pop, female_deaths).smooth_data()

    male_data_t = dataTransform_mortality(male_pop_t, male_deaths_t).smooth_data()
    female_data_t = dataTransform_mortality(female_pop_t, female_deaths_t).smooth_data()

    male_data_or = dataTransform_mortality(male_pop, male_deaths).mx.reset_index(drop=True).T.reset_index(drop=True).T
    female_data_or = dataTransform_mortality(female_pop, female_deaths).mx.reset_index(drop=True).T.reset_index(drop=True).T

    male_model_mort = mortModel(male_data, 6)
    female_model_mort = mortModel(female_data, 6)

    male_model_mort_t = mortModel(male_data_t, 6)
    female_model_mort_t = mortModel(female_data_t, 6)

    error_list = pd.Series()
    j = 0
    for i in range(800, 1001):
            test_phi = i/1000
            beta_test = male_model_mort_t.project(5, phi=test_phi)
            test_forecast = male_model_mort_t.forecasted_component(beta_test)
            error_frame = test_forecast.subtract(male_data_or, axis="columns")

            mse = np.pow(error_frame.stack().dropna().mean(), 2)

            error_list.loc[j] = mse
            j = j + 1

    phi_m = (error_list.idxmin()+800)/1000
    print(f"optimal phi: {phi_m}")

    error_list = pd.Series()
    j = 0
    for i in range(800, 1001):
            test_phi = i/1000
            beta_test = female_model_mort_t.project(5, phi=test_phi)
            test_forecast = female_model_mort_t.forecasted_component(beta_test)
            error_frame = test_forecast.subtract(female_data_or, axis="columns")

            mse = np.pow(error_frame.stack().dropna().mean(), 2)
            print(mse)

            error_list.loc[j] = mse
            j = j + 1

    phi_f = (error_list.idxmin()+800)/1000
    print(f"optimal phi: {phi_f}")

    male_beta_f = male_model_mort.project(30, phi_m)
    female_beta_f = female_model_mort.project(30, phi_f)

    forecast_mort_male = male_model_mort.forecasted_component(male_beta_f).T
    forecast_mort_female = female_model_mort.forecasted_component(female_beta_f).T

    forecast_mort_male["year"] = forecast_mort_male.index + 1990
    forecast_mort_female["year"] = forecast_mort_female.index + 1990

    forecast_mort_male.to_csv("male_mort_forecast.csv")
    forecast_mort_female.to_csv("female_mort_forecast.csv")
    
    for i in range(0, 16):
        plt.subplot(1,2,1)
        plt.plot(forecast_mort_male["year"], forecast_mort_male[i])
        plt.subplot(1,2,2)
        plt.plot(forecast_mort_female["year"], forecast_mort_female[i])
    plt.show()


if __name__ == "__main__":
    main()