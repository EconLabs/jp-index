import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.nonparametric.kernel_regression import KernelReg
from numpy.linalg import svd
from statsmodels.tsa.arima.model import ARIMA
from scipy.stats import boxcox
from statsmodels.nonparametric.smoothers_lowess import lowess
from statsmodels.tsa.holtwinters import Holt

class natModel():
    def __init__(self, tfr, n_components, error):
        self.tfr = np.log(tfr)
        self.n_components = n_components
        self.error = error
        self.num_age_groups = len(tfr.index)
        self.num_years = len(tfr.index)
        self.start_year = tfr.index[0]
        self.averages = self.data_averages()
        self.centered_data = self.centralized_frame()
    
    def data_averages(self):
        aggregates = list(self.tfr.T.sum())
        averages = []
        for i in range(0, len(self.tfr.index)):
            averages.append(float(aggregates[i] / self.tfr.index.size))
        return averages


    def centralized_frame(self):
        tfr_t = self.tfr.copy().reset_index(drop=True).T
        centered_matrix = pd.DataFrame(index = tfr_t.index)
        for i in range(len(self.averages)):
            centered_matrix[i] = pd.concat([pd.Series(tfr_t[i] - self.averages[i])], axis=1)
        return centered_matrix.T
    
    def age_effects(self):
        u, s, vt = np.linalg.svd(self.centered_data)
        u = pd.DataFrame(u)
        age_effects = pd.DataFrame()
        for i in range(0, self.n_components):
            age_effects[i] = u[i]
        return age_effects
        
    def year_effects(self):
        u, s, vt = np.linalg.svd(self.centered_data)
        vt = pd.DataFrame(vt)
        age_effects = pd.DataFrame()
        for i in range(0, self.n_components):
            age_effects[i] = vt.iloc[i]
        return age_effects
    
    def sing_vals(self):
        u, s, vt = np.linalg.svd(self.centered_data)
        s = pd.Series(s)
        sing_vals = pd.Series()
        for i in range(0, self.n_components):
            sing_vals[i] = s[i]
        return sing_vals
        
    
    def project(self, n_years, ext_effects = None):
        if ext_effects != None:
            y_effects = ext_effects
        else:
            y_effects = self.year_effects()

        # Exponential Smoothing State Space Model
        y_effects_f = pd.DataFrame()
        for i in y_effects.columns:
            model = Holt(y_effects[i], damped_trend=True)
            model_fit = model.fit()
            y_effects_f[i] = pd.concat([pd.Series(model_fit.forecast(n_years))])
        y_effects_f = pd.concat([y_effects, y_effects_f], axis=0)
        return y_effects_f
    

    def forecasted_component(self, projected_eff):
        age_effects = self.age_effects()
        year_effects = projected_eff
        sing_vals = self.sing_vals()
        averages = self.averages
        final_matrix = pd.DataFrame(np.zeros(shape=(self.num_age_groups, len(year_effects))))
        temp_matrix = pd.DataFrame()
        
        for k in range(0, self.n_components):
            year_temp = pd.Series(year_effects[k])
            age_temp = pd.Series(age_effects[k])
            for j in range(0, len(year_temp)):
                effects_i = year_temp[j] * age_temp * sing_vals[k]
                temp_matrix[j] = effects_i
            final_matrix = final_matrix + temp_matrix

        final_matrix = pd.DataFrame(final_matrix)
        final_matrix = final_matrix.T
        for i in final_matrix.columns:
            final_matrix[i] = np.exp(final_matrix[i] + averages[i])
        return final_matrix.T

class mortModel():
    def __init__(self, mx, n_components):
        self.mx = mx
        self.n_components = n_components
        self.num_age_groups = len(mx.index)
        self.num_years = len(mx.index)
        self.start_year = mx.index[0]
        self.averages = self.data_averages()
        self.centered_data = self.centralized_frame()
    
    def data_averages(self):
        aggregates = list(self.mx.T.sum())
        averages = []
        for i in range(0, len(self.mx.index)):
            averages.append(float(aggregates[i] / self.mx.index.size))
        return averages


    def centralized_frame(self):
        mx_t = self.mx.copy().reset_index(drop=True).T
        centered_matrix = pd.DataFrame(index = mx_t.index)
        for i in range(len(self.averages)):
            centered_matrix[i] = pd.concat([pd.Series(mx_t[i] - self.averages[i])], axis=1)
        return centered_matrix.T
    
    def age_effects(self):
        u, s, vt = np.linalg.svd(self.centered_data)
        u = pd.DataFrame(u)
        age_effects = pd.DataFrame()
        for i in range(0, self.n_components):
            age_effects[i] = u[i]
        return age_effects
        
    def year_effects(self):
        u, s, vt = np.linalg.svd(self.centered_data)
        vt = pd.DataFrame(vt)
        year_effects = pd.DataFrame()
        for i in range(0, self.n_components):
            year_effects[i] = vt.iloc[i]
        return year_effects
    
    def sing_vals(self):
        u, s, vt = np.linalg.svd(self.centered_data)
        s = pd.Series(s)
        sing_vals = pd.Series()
        for i in range(0, self.n_components):
            sing_vals[i] = s[i]
        return sing_vals
        
    
    def project(self, n_years, ext_effects = None):
        if ext_effects != None:
            y_effects = ext_effects
        else:
            y_effects = self.year_effects()

        # Exponential Smoothing State Space Model
        y_effects_f = pd.DataFrame()
        for i in y_effects.columns:
            model = Holt(y_effects[i], damped_trend=True)
            model_fit = model.fit()
            y_effects_f[i] = pd.concat([pd.Series(model_fit.forecast(n_years))])
        y_effects_f = pd.concat([y_effects, y_effects_f], axis=0)
        return y_effects_f
    

    def forecasted_component(self, projected_eff=None, n_years=None):
        age_effects = self.age_effects()
        if projected_eff == None:
            year_effects = self.project(n_years)
        else:
            year_effects = projected_eff
        sing_vals = self.sing_vals()
        averages = self.averages
        final_matrix = pd.DataFrame(np.zeros(shape=(self.num_age_groups, len(year_effects))))
        temp_matrix = pd.DataFrame()
        
        for k in range(0, self.n_components):
            year_temp = pd.Series(year_effects[k])
            age_temp = pd.Series(age_effects[k])
            for j in range(0, len(year_temp)):
                effects_i = year_temp[j] * age_temp * sing_vals[k]
                temp_matrix[j] = effects_i
            final_matrix = final_matrix + temp_matrix

        final_matrix = pd.DataFrame(final_matrix)
        final_matrix = final_matrix.T
        for i in final_matrix.columns:
            final_matrix[i] = final_matrix[i] + averages[i]
        return final_matrix.T
    
class dataTransform_fertility():
    def __init__(self, fem_pop, births):
        self.births = births
        self.fem_pop = fem_pop
        self.columns = fem_pop.columns
        self.tfr = self.fertility_rate()
        self.num_age_groups = len(fem_pop.columns)
        self.fit_data = self.box_cox_fit()
        self.fit_lambda = self.lambda_fit()

    def fertility_rate(self):
        tfr = pd.DataFrame(index=self.fem_pop.index)
        for i in self.columns:
            tfr[i] = pd.concat([self.births[i] / self.fem_pop[i]])
        return tfr
    
    def box_cox_fit(self):
        fitted_data = pd.DataFrame()
        for i in range(0, self.num_age_groups):
            fit, lamb = boxcox(self.tfr[self.columns[i]])
            fitted_data[i] = pd.concat([pd.Series(fit)], axis=1)
        fitted_data = fitted_data.set_index(self.tfr.index)
        return fitted_data
    
    def lambda_fit(self):
        fitted_lambda = pd.Series()
        for i in range(0, self.num_age_groups):
            fit, lamb = boxcox(self.tfr[self.columns[i]])
            fitted_lambda[i] = lamb
        return fitted_lambda

    def kernel_transform(self):
        num_age_groups = len(self.fit_data.columns)
        tfr_transform = pd.DataFrame()
        marginal_effects = pd.DataFrame()
        rsquared = pd.Series()
        for i in range(0, num_age_groups):
            model = KernelReg(self.fit_data[i], self.fit_data[i], var_type='c')
            tfr_hat, m_effects = model.fit()
            rsquared[i] = model.r_squared()
            tfr_transform[i] = pd.concat([pd.Series(tfr_hat)], axis=1)
            marginal_effects[i] = pd.concat([pd.DataFrame(m_effects)], axis=1)
        tfr_transform = tfr_transform.set_index(self.fit_data.index)
        return tfr_transform, marginal_effects, rsquared

    def errors(self):
        variance = pd.DataFrame(index=self.tfr.index)
        j = 0
        for i in self.columns:
            lambda_err = self.births[i].sum(axis=0) / self.fem_pop[i].sum(axis=0)
            exp = 2*lambda_err - 1
            variance[j] = pd.concat([(self.tfr[i]**exp)*(self.fem_pop[i].astype("float") ** -1)])
            j += 1

        return variance

class dataTransform_mortality():
    def __init__(self, pop, deaths):
        self.pop = pop
        self.deaths = deaths
        self.mx = pd.DataFrame(deaths/pop)

    def smooth_data(self):
        mx_s = pd.DataFrame()
        for i in self.mx.columns:
            temp = self.mx[i].values
            mx_s[i] = pd.Series(lowess(temp, range(len(temp)), frac=0.2, return_sorted=False))
        return mx_s

def main():
    nat_data = pd.read_csv("births.csv").set_index("year")
    female_pop_fert = pd.read_csv("fem_pop.csv").set_index("year")
    male_pop = pd.read_csv("exposure_male.csv").set_index("year")
    female_pop = pd.read_csv("exposure_female.csv").set_index("year")
    male_deaths = pd.read_csv("deaths_male.csv").set_index("year")
    female_deaths = pd.read_csv("deaths_female.csv").set_index("year")

    nat_data = nat_data.T
    female_pop_fert = female_pop_fert.T
    data_transformed = dataTransform_fertility(female_pop_fert, nat_data)
    nat_model = natModel(data_transformed.tfr, 6, data_transformed.errors())

    male_pop = male_pop.T
    male_deaths = male_deaths.T
    female_pop = female_pop.T
    female_deaths = female_deaths.T
    male_mx = dataTransform_mortality(male_pop, male_deaths).smooth_data()
    female_mx = dataTransform_mortality(female_pop, female_deaths).mx

    male_model_mort = mortModel(male_mx, 6)
    female_model_mort = mortModel(female_mx, 6)

    nat_model.forecasted_component(nat_model.project(30)).to_csv("forecasted_tfr.csv")


if __name__ == "__main__":
    main()
