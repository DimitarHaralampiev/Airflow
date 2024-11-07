import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from plugins.load.load_to_csv import load_to_csv


class TestLoadToCsv(unittest.TestCase):

    @patch("plugins.load.pd.DataFrame.to_csv")
    def test_load_to_csv(self, mock_to_csv):
        # Create a sample DataFrame
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"]
        })

        # Define the path where the CSV should be saved
        csv_file_path = "test_output.csv"

        # Call the load_to_csv task
        load_to_csv(df, csv_file_path)

        # Assert that to_csv was called with the correct file path
        mock_to_csv.assert_called_once_with(csv_file_path)


if __name__ == "__main__":
    unittest.main()