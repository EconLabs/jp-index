import polars as pl

from .data_pull import DataPull


class DataIndex(DataPull):
    """
    Data processing class that calculates multiple indicators from the DataPull class
    """

    def __init__(
        self,
        saving_dir: str = "data/",
        database_file: str = "data.ddb",
        log_file: str = "data_process.log",
    ):
        """
        Initialize the DataIndex class.

        Parameters
        ----------
        saving_dir: str
            Directory to save the data.
        database_file: str
            file path that will save the duckdb instance
        log_file: str
            file path that will save the log messegases

        Returns
        -------
        None
        """
        super().__init__(saving_dir, database_file, log_file)

    def consumer_data(self, time_frame: str) -> pl.DataFrame:
        """
        Aggregate consumer data based on the specified time frame.

        Parameters
        ----------
        time_frame: str
            The time frame for aggregation. Valid options are:
            - "monthly": No aggregation, returns the raw data.
            - "quarterly": Aggregates data by year and quarter.
            - "yearly": Aggregates data by year.
            - "fiscal": Aggregates data by fiscal period.

        Returns
        -------
        pl.DataFrame
            A Polars DataFrame with aggregated data based on the specified time frame.

        Raises
        ------
        ValueError
            If an invalid time frame is provided.
        """
        df = self.insert_consumer()
        variables = df.columns
        remove = ["date", "month", "year", "quarter", "fiscal"]
        variables = [var for var in variables if var not in remove]
        aggregation_exprs = [pl.col(var).sum().alias(var) for var in variables]

        match time_frame:
            case "monthly":
                return df
            case "quarterly":
                return df.group_by(["year", "quarter"]).agg(aggregation_exprs)
            case "yearly":
                return df.group_by("year").agg(aggregation_exprs)
            case "fiscal":
                return df.group_by("fiscal").agg(aggregation_exprs)
            case _:
                raise ValueError("Invalid aggregation")

    def jp_indicator_data(self, time_frame: str) -> pl.DataFrame:
        """
        Aggregate consumer data based on the specified time frame.

        Parameters
        ----------
        time_frame: str
            The time frame for aggregation. Valid options are:
            - "monthly": No aggregation, returns the raw data.
            - "quarterly": Aggregates data by year and quarter.
            - "yearly": Aggregates data by year.
            - "fiscal": Aggregates data by fiscal period.

        Returns
        -------
        pl.DataFrame
            A Polars DataFrame with aggregated consumer data based on the specified time frame.

        Raises
        ------
        ValueError
            If an invalid time frame is provided.
        """

        df = self.insert_jp_index()
        variables = df.columns
        remove = ["date", "month", "year", "quarter", "fiscal"]
        variables = [var for var in variables if var not in remove]
        aggregation_exprs = [pl.col(var).sum().alias(var) for var in variables]

        match time_frame:
            case "monthly":
                return df
            case "quarterly":
                return df.group_by(["year", "quarter"]).agg(aggregation_exprs)
            case "yearly":
                return df.group_by("year").agg(aggregation_exprs)
            case "fiscal":
                return df.group_by("fiscal").agg(aggregation_exprs)
            case _:
                raise ValueError("Invalid aggregation")
