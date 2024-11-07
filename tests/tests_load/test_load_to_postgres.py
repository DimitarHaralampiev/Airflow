import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

from plugins.load.load_to_posgresql import load_to_postgres

class TestLoadToPostgres(unittest.TestCase):

    @patch("data_utils.get_engine")
    def test_load_to_postgres_success(self, mock_get_engine):
        # Mock the engine and the DataFrame
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_df = MagicMock(spec=pd.DataFrame)

        # Call the function
        table_name = "test_table"
        load_to_postgres(mock_df, table_name)

        # Assertions
        mock_get_engine.assert_called_once_with("postgres", "postgres", "localhost", 5432, "etl")
        mock_df.to_sql.assert_called_once_with(table_name, con=mock_engine, if_exists='replace', index=False)
        mock_engine.dispose.assert_called_once()

    @patch("data_utils.get_engine")
    def test_load_to_postgres_engine_creation_failed(self, mock_get_engine):
        # Set get_engine to return None to simulate engine creation failure
        mock_get_engine.return_value = None
        mock_df = MagicMock(spec=pd.DataFrame)

        # Call the function
        table_name = "test_table"
        load_to_postgres(mock_df, table_name)

        # Assertions
        mock_get_engine.assert_called_once()
        mock_df.to_sql.assert_not_called()  # to_sql should not be called if engine creation fails

    @patch("data_utils.get_engine")
    @patch("data_utils.logger")
    def test_load_to_postgres_sqlalchemy_error(self, mock_logger, mock_get_engine):
        # Mock the engine and DataFrame, and simulate SQLAlchemy error
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_df = MagicMock(spec=pd.DataFrame)
        mock_df.to_sql.side_effect = SQLAlchemyError("Test error")

        # Call the function
        table_name = "test_table"
        load_to_postgres(mock_df, table_name)

        # Assertions
        mock_df.to_sql.assert_called_once_with(table_name, con=mock_engine, if_exists='replace', index=False)
        mock_logger.error.assert_called_with("An error occurred while writing to the 'test_table' table: Test error")
        mock_engine.dispose.assert_called_once()


if __name__ == "__main__":
    unittest.main()