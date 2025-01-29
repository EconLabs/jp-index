import pandas as pd
import numpy as np
from statsmodels.tsa.arima.model import ARIMA

class mortModel():
    def __init__(self, deaths, exposure):
        # Declare given dataframes
        self.deaths = deaths
        self.exposure = exposure
        # Calculate death rates
        self.death_rate = self.deaths.divide(self.exposure)
        # Create logged death rate matrix
        self.log_death_rate = np.log(self.death_rate)
    
    def mort_constants(self):
        # Aggregate the logged death rates by age groups
        log_aggregates = list(self.log_death_rate.T.sum())
        averages = []
        # Start loop in range of 0 to the length of the index (number of age groups) of the original logged death rates DataFrame
        for i in range(0, self.log_death_rate.index.size):
            # Calculate average logged death rate for age group i and append value to empty list
            averages.append(float(log_aggregates[i] / len(self.log_death_rate.columns.values)))
        return averages
        

    def centralized_matrix(self):
        # Create empty matrix with the years from the logged death rates matrix as the index
        centered_matrix = pd.DataFrame(index = self.log_death_rate.columns.values)
        # Copy the logged death rates matrix, reset the index to turn the age groups to numbers, and transpose so the age groups are on top
        logged_rates = self.log_death_rate.copy().reset_index(drop=True).T
        for i in range(len(self.mort_constants())):
            # Calculate the centralized death rate at age group i by subtracting the mortality constant of group i from the values of the column of age group i and add to matrix
            centered_matrix[i] = pd.concat([pd.Series(logged_rates[i] - self.mort_constants()[i])], axis = 1)
        # Return transposed matrix so years are on top again
        return centered_matrix.T
    

    def age_constants(self):
        # Call the centralized matrix method and turn into numpy array to prepare for SVD
        matrix = self.centralized_matrix().to_numpy()
        # Perform SVD and declare the resulting values
        U,S,V = np.linalg.svd(matrix)
        # Create a dataframe from the relevant value
        age_constant_list = pd.DataFrame(U)
        # Turn the first column of values of the age constant matrix into a series and return it
        age_constant_list = pd.Series(age_constant_list[0])
        return age_constant_list
    

    def year_constants(self):
        # Call the centralized matrix method and turn into numpy array to prepare for SVD
        matrix = self.centralized_matrix().to_numpy()
        # Perform SVD and declare the resulting values
        U,S,V = np.linalg.svd(matrix)
        # Create a dataframe from the relevant value
        year_constants = pd.DataFrame(V)
        # Turn the values of the first row into a series and return it
        year_constants = pd.Series(year_constants.iloc[0])
        return year_constants
    

    def singular_values(self):
        # Call the centralized matrix method and turn into numpy array to prepare for SVD
        matrix = self.centralized_matrix().to_numpy()
        # Perform SVD and declare the resulting values
        U,S,V = np.linalg.svd(matrix)
        # Turn the array of scalars into a list and return the first one (the scaling eigenvalue)
        vector = list(S)
        return vector[0]
    

    def mortality_rate(self, projected_constants):
        # Create empty DataFrame
        mortality = pd.DataFrame()
        for i in range(0, len(projected_constants)):
            # Create empty values list for use in loop
            values_list = []
            for j in range(0,16):
                # Calculate mortality rate from formula (mx,t = ax + s1*bx*kt) and append result to empty list
                values_list.append(np.exp(self.mort_constants()[j] + (self.singular_values()*self.age_constants()[j]*projected_constants[i])))
                print(f"Age group {j} for year {i+int(self.death_rate.columns.values[0])} done")
            # Add list of results to the mortality DataFrame
            mortality[i+int(self.death_rate.columns.values[0])] = pd.concat([pd.Series(values_list)], axis=1)
        return mortality
    
    def year_constants_projection(self, n_years, p=0, d=1, q=0, ext_constants = None):
        if ext_constants != None:
            y_constants = ext_constants
        else:
            y_constants = self.year_constants()
        
        # Arima
        model = ARIMA(y_constants, order=(p,d,q))
        model_fit = model.fit()
        y_constants_f = model_fit.forecast(n_years)
        y_constants_f = pd.Series(y_constants_f)
        y_constants_f = y_constants._append(y_constants_f, ignore_index=True)
        return y_constants_f

        


def main():
    deaths_male = pd.read_csv("deaths_male.csv").set_index("age_group")
    exposure_male = pd.read_csv("exposure_male.csv").set_index("age_group")
    deaths_female = pd.read_csv("deaths_female.csv").set_index("age_group")
    exposure_female = pd.read_csv("exposure_female.csv").set_index("age_group")
    lc_m = mortModel(deaths_male, exposure_male)
    lc_f = mortModel(deaths_female, exposure_female)
    #lc_m.mortality_rate(lc_m.year_constants_projection(30)).to_csv("male_mortality_rate_2050.csv")
    #lc_f.mortality_rate(lc_f.year_constants_projection(30)).to_csv("female_mortality_rate_2050.csv")

if __name__ == "__main__":
    main()
