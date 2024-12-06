import pandas as pd
import numpy as np
from scipy import linalg
import matplotlib.pyplot as plt
from statsmodels.nonparametric.kernel_regression import KernelReg
from sklearn.decomposition import PCA
from numpy.linalg import svd
from statsmodels.tsa.arima.model import ARIMA
from scipy.stats import boxcox

class natModel():
    def __init__(self, tfr, n_components):
        self.tfr = tfr
        self.n_components = n_components
        self.num_age_groups = len(tfr.columns)
        self.num_years = len(tfr.index)
        self.start_year = tfr.index[0]
        self.averages = self.nat_averages()
        self.centered_data = self.centralized_frame()
    
    def nat_averages(self):
        aggregates = list(self.tfr.sum())
        averages = []
        for i in range(0, len(self.tfr.columns.values)):
            averages.append(float(aggregates[i] / self.tfr.index.size))
        return averages


    def centralized_frame(self):
        centered_matrix = pd.DataFrame(index = self.tfr.index)
        tfr_t = self.tfr.copy().T.reset_index(drop=True).T
        for i in range(len(self.averages)):
            centered_matrix[i] = pd.concat([pd.Series(tfr_t[i] - self.averages[i])], axis=1)
        return centered_matrix
    
    def age_effects(self):
        age_effects = pd.DataFrame()
        u, s, vt = svd(self.centered_data)
        vt = pd.DataFrame(vt)
        for i in range(0, self.n_components):
            age_effects[i] = pd.concat([vt.iloc[i]])
        return age_effects
        
    def year_effects(self):
        year_effects = pd.DataFrame()
        u, s, vt = svd(self.centered_data)
        u = pd.DataFrame(u)
        for i in range(0, self.n_components):
            year_effects[i] = pd.concat([u[i]])
        return year_effects
        
    def sing_vals(self):
        singular_values = pd.Series()
        u, s, vt = svd(self.centered_data)
        s = pd.Series(s)
        for i in range(0, self.n_components):
            singular_values[i] = s[i]
        return singular_values
    
    def project(self, n_years, p=0, d=1, q=0, ext_effects = None):
        if ext_effects != None:
            y_effects = ext_effects
        else:
            y_effects = self.year_effects()

        # Arima
        y_effects_f = pd.DataFrame()
        for i in y_effects:
            model = ARIMA(y_effects[i], order=(p,d,q))
            model_fit = model.fit()
            y_effects_f[i] = pd.concat([pd.Series(model_fit.forecast(n_years))])
        y_effects_f = pd.concat([y_effects, y_effects_f], axis=0)
        return y_effects_f
    

    def forecasted_tfr(self):
        coefficients = ...


class dataTransform():
    def __init__(self, tfr, fem_pop):
        self.tfr = tfr
        self.fem_pop = fem_pop
        self.fem_columns = self.fem_pop.columns
        self.columns =  self.tfr.columns
        self.num_age_groups = len(tfr.columns)
        self.fit_data = self.box_cox_fit()
        self.fit_lambda = self.lambda_fit()

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
        variance_list = pd.DataFrame()
        for i in range(0, self.num_age_groups):
            exp = (2 * self.fit_lambda[i]) - 1
            variance = (self.tfr.T.sum() ** exp) * (self.fem_pop.T.sum().astype("float") ** -1)
            variance_list[i] = pd.concat([pd.Series(variance)], axis=1)
        return variance_list

    

def main():
    nat_data = pd.read_csv("tfr_data.csv").set_index("year")
    female_pop = pd.read_csv("fem_pop.csv").set_index("year")
    transformed = dataTransform(nat_data, female_pop)
    print(transformed.errors())

    #tfr_transform, marginal_effects, rsquared = dataTransform(nat_data).kernel_transform()
    #nat_model = natModel(tfr_transform, 4)

    #tfr_projection = nat_model.project(n_years=30)
    #tfr_projection.plot()
    #plt.show()
    #err = nat_model.centralized_frame() - pred_tfr
    #j = 0
    #for i in rsquared:
    #    print(f"R^2 of age group {j}: {i}")
    #    j =+ 1

    # Plot
    #fig, axes = plt.subplots(nrows = 1, ncols=3)
    #nat_model.centralized_frame().plot(ax=axes[0], title="Original Centralized TFR")
    #pred_tfr.plot(ax=axes[1], title="Kernel Regression TFR")
    #err.plot(ax=axes[2], title="Kernel Regression Errors vs Original Data")
    #plt.show()
    
if __name__ == "__main__":
    main()
