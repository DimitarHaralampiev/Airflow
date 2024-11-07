import unittest
import pandas as pd
from plugins.transform.transform import (
    drop_duplication,
    drop_na,
    rename_columns,
    drop_columns,
    change_types,
    clean_bag_weight_col,
    reset_index,
)

class TestETLFunctions(unittest.TestCase):

    def setUp(self):
        # Sample DataFrame for testing
        self.df = pd.DataFrame({
            "Species": ["Arabica", "Arabica", "Robusta"],
            "Owner": ["Owner1", "Owner1", "Owner2"],
            "Altitude": [1200, 1200, 900],
            "Bag_Weight": ["60 kg", "70 kg", "50 kg"],
            "Harvest.Year": [2018, 2018, 2019],
            "Certification_Contact": ["Contact1", "Contact1", "Contact2"],
            "Lot.Number": ["A1", "A1", "B1"],
            "Grading_Date": ["2021-01-01", "2021-01-01", "2021-01-02"],
            "Expiration": ["2023-01-01", None, "2024-01-01"]
        })

    def test_drop_duplication(self):
        df_result = drop_duplication(self.df)
        expected_df = self.df.drop_duplicates()
        pd.testing.assert_frame_equal(df_result, expected_df)

    def test_drop_na(self):
        df_result = drop_na(self.df)
        expected_df = self.df.dropna()
        pd.testing.assert_frame_equal(df_result, expected_df)

    def test_rename_columns(self):
        df_result = rename_columns(self.df)
        expected_df = self.df.copy()
        expected_df.columns = ["species", "owner", "altitude", "bag_weight", "harvest_year", "certification_contact", "lot_number", "grading_date", "expiration"]
        pd.testing.assert_frame_equal(df_result, expected_df)

    def test_drop_columns(self):
        df_result = drop_columns(self.df)
        expected_df = self.df.drop(columns=["Altitude", "Certification_Contact"])
        pd.testing.assert_frame_equal(df_result, expected_df)

    def test_change_types(self):
        df = self.df.copy()
        df["Grading_Date"] = pd.to_datetime(df["Grading_Date"])
        df["Expiration"] = pd.to_datetime(df["Expiration"])

        df_result = change_types(df)

        # Check that categorical columns are of type 'category'
        for col in ["Species", "Owner", "Lot.Number"]:
            self.assertTrue(pd.api.types.is_categorical_dtype(df_result[col]))

        # Check that date columns are of type datetime
        for col in ["Harvest.Year", "Grading_Date", "Expiration"]:
            self.assertTrue(pd.api.types.is_datetime64_any_dtype(df_result[col]))

    def test_clean_bag_weight_col(self):
        df_result = clean_bag_weight_col(self.df, "Bag_Weight")
        expected_df = self.df.copy()
        expected_df["Bag_Weight"] = pd.to_numeric(expected_df["Bag_Weight"].str.split(" ").str.get(0), errors="coerce")
        pd.testing.assert_frame_equal(df_result, expected_df)

    def test_reset_index(self):
        df_result = reset_index(self.df)
        expected_df = self.df.reset_index()
        pd.testing.assert_frame_equal(df_result, expected_df)

if __name__ == "__main__":
    unittest.main()