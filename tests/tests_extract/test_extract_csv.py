import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from plugins.extract.extract_csv import read_csv_file

class TestReadCSV(unittest.TestCase):

    @patch('plugins.extract.extract_csv.read_csv_file')
    def test_successful_read(self, mock_read_csv):
        mock_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        mock_read_csv.return_value = mock_df

        result_df = read_csv_file("/home/dimitar/Data Science/DataTidiyngAndClening/data/arabica_data_cleaned.csv")

        pd.testing.assert_frame_equal(result_df, mock_df)
        mock_read_csv.assert_called_once_with("/home/dimitar/Data Science/DataTidiyngAndClening/data/arabica_data_cleaned.csv")

    @patch('plugins.extract.extract_csv.read_csv_file')
    def read_test_csv_error(self, mock_read_csv):
        mock_read_csv.side_effect = Exception("File not found")

        with self.assertRaises(Exception) as context:
            read_csv_file("/home/dimitar/Data Science/DataTidiyngAndClening/data/arabica_data_cleaned.csv")

        self.assertEqual(str(context.exception), "File not found")
        mock_read_csv.assert_called_once_with("/home/dimitar/Data Science/DataTidiyngAndClening/data/arabica_data_cleaned.csv")


if __name__ == '__main__':
    unittest.main()