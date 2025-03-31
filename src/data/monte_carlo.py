import pandas as pd
from scipy.stats import truncnorm
from tqdm import tqdm

class monteCarlo():
    def __init__(self, simulation_count):
        self.simulation_count = simulation_count

    def simulate_normal(self, years, year_constants, upper, lower, std_dev):
        sim_values = pd.DataFrame()
        years = pd.Series(years)
        counter = 0
        for i in year_constants:
            for _ in tqdm(range(1)):
                values_list = truncnorm.rvs((lower - i)/std_dev, (upper - i)/std_dev, loc=i, scale=std_dev, size=self.simulation_count)
            sim_values[f"{years[counter]}"] = pd.Series(values_list)
            print(f"Year {years[counter]} done")
            counter += 1
        return sim_values
    
    def percentiles(self, sim_results):
        all_percentiles = pd.DataFrame()
        for column in sim_results:
            perc_1 = float(sim_results[f"{column}"].quantile(0.005))
            perc_3 = float(sim_results[f"{column}"].quantile(0.025))
            perc_5 = float(sim_results[f"{column}"].quantile(0.05))
            perc_50 = float(sim_results[f"{column}"].quantile(0.5))
            perc_95 = float(sim_results[f"{column}"].quantile(0.95))
            perc_97 = float(sim_results[f"{column}"].quantile(0.975))
            perc_99 = float(sim_results[f"{column}"].quantile(0.995))
            percentiles = pd.Series([perc_1, perc_3, perc_5, perc_50, perc_95, perc_97, perc_99])
            all_percentiles[f"{column}"] = percentiles
        return all_percentiles.T

def main():
    # Male year constants simulation
    std_dev_m = 0.2996471
    std_dev_f = 0.3225237
    upper_normal_m = .8
    lower_normal_m = -0.3236444
    upper_normal_f = .8
    lower_normal_f = -0.25307


    year_constants_p = pd.read_csv("kt_p.csv")
    mc = monteCarlo(1000000)
    mc.percentiles(mc.simulate_normal(year_constants_p["year"], year_constants_p["kt_m_p"], upper_normal_m, lower_normal_m, std_dev_m)).to_csv("kt_sim_m.csv")
    mc.percentiles(mc.simulate_normal(year_constants_p["year"], year_constants_p["kt_f_p"], upper_normal_f, lower_normal_f, std_dev_f)).to_csv("kt_sim_f.csv")
    

if __name__ == "__main__":
    main()
