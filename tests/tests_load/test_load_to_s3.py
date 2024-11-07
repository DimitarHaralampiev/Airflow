import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from io import StringIO

from plugins.load.load_to_s3 import load_to_s3

class TestLoadToS3(unittest.TestCase):

    @patch("plugins.boto3.client")
    def test_load_to_s3_success(self, mock_boto_client):
        # Mock the S3 client and the DataFrame
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client

        # Create a sample DataFrame
        sample_data = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(sample_data)

        # Call the function
        s3_bucket = "test-bucket"
        s3_key = "test-key.csv"
        load_to_s3(df, s3_bucket, s3_key)

        # Assertions
        mock_boto_client.assert_called_once_with("s3", region_name="eu-south-1")
        mock_s3_client.put_object.assert_called_once()

        # Check the put_object call to verify the bucket and key
        args, kwargs = mock_s3_client.put_object.call_args
        self.assertEqual(kwargs["Bucket"], s3_bucket)
        self.assertEqual(kwargs["Key"], s3_key)
        self.assertIsInstance(kwargs["Body"], StringIO)  # Check if the body is a StringIO instance

    @patch("plugins.boto3.client")
    def test_load_to_s3_exception(self, mock_boto_client):
        # Mock the S3 client to raise an exception
        mock_s3_client = MagicMock()
        mock_s3_client.put_object.side_effect = Exception("Test S3 upload error")
        mock_boto_client.return_value = mock_s3_client

        # Create a sample DataFrame
        sample_data = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(sample_data)

        # Call the function and check for printed error
        s3_bucket = "test-bucket"
        s3_key = "test-key.csv"

        with self.assertLogs(level="ERROR") as log:
            load_to_s3(df, s3_bucket, s3_key)

        # Assertions
        mock_boto_client.assert_called_once_with("s3", region_name="eu-south-1")
        mock_s3_client.put_object.assert_called_once()
        self.assertIn("An error occurred: Test S3 upload error", log.output[0])


if __name__ == "__main__":
    unittest.main()
