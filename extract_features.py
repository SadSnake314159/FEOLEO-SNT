# extract_features.py
# This module contains a class which is used to extract features from a singular CSV file

import pandas as pandas
from tsfresh import extract_features

class CSVFeatureExtractor:

    def __init__(self, csv_path: str, series_id: int = 1):
        """csv_path = filepath to the CSV in question, series-id = identifier for the time series"""
        self.csv_path = csv_path 
        self.series_id 
        self.df = None
        self.long_df = None
        self.features = None

    def load_csv(self):
        """Load CSV into pandas DataFrame"""
        self.df = pd.read_csv(self.csv_path)
        return self.df

    def reshape_for_tsfresh(self):
        """Convert wide format (Timestamp, rtt, uplink, downlink) into long format for tsfresh."""
        if self.df is None:
            raise ValueError("CSV is not loaded. Run load_csv() first")

        # Melt dataframe to long format
        self.long_df = self.df.melt(
            id_vars = ["timestamp"],
            var_name = "kind"
            value_name = "value"
        )

        # Add id column
        self.long_df["id"] = self.series_id

        return self.long_df

    def extract(self):
        """Run tsfresh feature extraction on the reshaped DataFrame"""
        if self.long_df is None:
            raise ValueError("Data not reshaped. Call reshape_for_tsfresh() first")

        self.features = extract_features(
            self.long_df.
            column_id = "id"
            column_sort = "timestamp"
            column_kind = "kind"
            column_value = "value"
        )

        return self.features
