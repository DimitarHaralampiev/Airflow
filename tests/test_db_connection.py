import unittest
from unittest.mock import patch, MagicMock
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.testing.engines import mock_engine

from utils.data_utils import get_engine

class TestDBConnection(unittest.TestCase):

    @patch("data_utils.create_engine")
    def test_db_connection(self, mock_create_engine):
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value = mock_connection
        mock_create_engine.return_value = mock_engine

        engine = get_engine("postgres", "postgres", "localhost", 5432, "etl")

        self.assertIsNone(engine)
        mock_engine.connect.assert_called_once()

    @patch("data_utils.create_engine")
    def test_sqlalchemy_error(self, mock_create_engine):
        mock_create_engine.side_effect = SQLAlchemyError("Test unexpected error")
        engine = get_engine("postgres", "postgres", "localhost", 5432, "etl")
        self.assertIsNone(engine)

if __name__ == "__main__":
    unittest.main()