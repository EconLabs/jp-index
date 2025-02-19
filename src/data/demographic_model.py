import pandas as pd
import numpy as np
from numpy.linalg import svd
from scipy.stats import boxcox
from statsmodels.nonparametric.smoothers_lowess import lowess
from statsmodels.tsa.holtwinters import Holt
from scipy.special import inv_boxcox

class natModel():
    def __init__(self, tfr, n_components):
        self.tfr = tfr
        self.n_components = n_components
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
        
    
    def project(self, n_years, phi=0.98):
        y_effects = self.year_effects()

        # Exponential Smoothing State Space Model
        y_effects_f = pd.DataFrame()
        for i in y_effects.columns:
            model = Holt(y_effects[i], damped_trend=True)
            model_fit = model.fit(damping_trend=phi)
            y_effects_f[i] = pd.concat([pd.Series(model_fit.forecast(n_years))])
        y_effects_f = pd.concat([y_effects, y_effects_f], axis=0)
        return y_effects_f
    

    def forecasted_component(self, l, projected_eff=None, n_years=None):
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
            final_matrix[i] = final_matrix[i] + averages[i]
        final_matrix = inv_boxcox(final_matrix.T, l)
        return final_matrix


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
        
    
    def project(self, n_years, phi=0.98):
        y_effects = self.year_effects()

        # Exponential Smoothing State Space Model
        y_effects_f = pd.DataFrame()
        for i in y_effects.columns:
            model = Holt(y_effects[i], damped_trend=True)
            model_fit = model.fit(damping_trend=phi)
            y_effects_f[i] = pd.concat([pd.Series(model_fit.forecast(n_years))])
        y_effects_f = pd.concat([y_effects, y_effects_f], axis=0)
        return y_effects_f
    

    def forecasted_component(self, projected_eff=None, n_years=None):
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
            final_matrix[i] = final_matrix[i] + averages[i]
        return np.exp(final_matrix.T)
    
class dataTransform_fertility():
    def __init__(self, fem_pop, births, lmbda):
        self.fem_pop = fem_pop
        self.births = births
        self.lmbda = lmbda
        self.tfr = pd.DataFrame(self.births/self.fem_pop)
    

    def box_cox(self):
        tfr_bc = boxcox(self.tfr, lmbda=self.lmbda)
        return pd.DataFrame(tfr_bc)


class dataTransform_mortality():
    def __init__(self, pop, deaths):
        self.pop = pop
        self.deaths = deaths
        self.mx = pd.DataFrame(deaths/pop)
        self.mx_l = np.log(self.mx)


    def smooth_data(self):
        mx_s = pd.DataFrame()
        for i in self.mx_l.columns:
            temp = self.mx_l[i].values
            mx_s[i] = pd.Series(lowess(temp, range(len(temp)), frac=0.2, return_sorted=False))
        return mx_s


def main():
    nat_data = pd.read_csv("births.csv").set_index("year")
    female_pop_fert = pd.read_csv("fem_pop.csv").set_index("year")
    male_pop = pd.read_csv("exposure_male.csv").set_index("year")
    female_pop = pd.read_csv("exposure_female.csv").set_index("year")
    male_deaths = pd.read_csv("deaths_male.csv").set_index("year")
    female_deaths = pd.read_csv("deaths_female.csv").set_index("year")


if __name__ == "__main__":
    main()
