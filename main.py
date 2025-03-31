from src.data.data_process import DataIndex
from src.data.data_pull import DataPull


def main() -> None:
    d = DataPull()
    for year in range(2013, 2025):
        d.pull_awards_by_year(year=year)


if __name__ == "__main__":
    main()
