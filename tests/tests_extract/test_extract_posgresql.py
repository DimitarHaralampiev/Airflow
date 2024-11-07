import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from plugins.extract.extract_postgres import extract_from_postgresql  # Replace 'your_module' with the actual module name


class TestExtractFromPostgresql(unittest.TestCase):

    @patch("data_utils.create_engine")
    @patch("data_utils.pd.read_sql")
    def test_successful_extraction(self, mock_read_sql, mock_create_engine):
        # Mock DataFrame to return
        mock_df = pd.DataFrame({"id": [1, 2], "title": ["Movie A", "Movie B"]})
        mock_read_sql.return_value = mock_df

        # Mock engine and connection
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        # Call the function
        result_df = extract_from_postgresql()

        # Verify the result matches the mock DataFrame
        pd.testing.assert_frame_equal(result_df, mock_df)
        # Check that `pd.read_sql` was called with the correct query and engine
        mock_read_sql.assert_called_once_with("SELECT * FROM movie_movie", mock_engine)
        # Check that `create_engine` was called once with the connection string
        mock_create_engine.assert_called_once_with("postgresql+psycopg2://postgres:postgres@localhost:5432/MovieAppNew")

    @patch("data_utils.create_engine")
    @patch("data_utils.pd.read_sql")
    def test_extraction_error(self, mock_read_sql, mock_create_engine):
        # Mock engine and connection
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        # Simulate an exception in `pd.read_sql`
        mock_read_sql.side_effect = Exception("Database connection error")

        # Call the function and check that it returns None due to the exception
        result_df = extract_from_postgresql()
        self.assertIsNone(result_df)

        # Check that `pd.read_sql` was called and caused an exception
        mock_read_sql.assert_called_once_with("SELECT * FROM movie_movie", mock_engine)
        # Check that `create_engine` was called once
        mock_create_engine.assert_called_once_with("postgresql+psycopg2://postgres:postgres@localhost:5432/MovieAppNew")


if __name__ == "__main__":
    unittest.main()