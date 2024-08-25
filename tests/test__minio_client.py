import pytest
import os
from unittest.mock import patch, MagicMock
from data_pipeline.minio_client import create_bucket_if_not_exists, upload_file, download_file

@patch('data_pipeline.minio_client.minio_client')
def test_create_bucket_if_not_exists(mock_minio):
    
    mock_minio.bucket_exists.return_value = False
    
    bucket_name = "test-bucket"
    
    create_bucket_if_not_exists(bucket_name)
    
    mock_minio.make_bucket.assert_called_once_with(bucket_name)

    mock_minio.reset_mock()

    mock_minio.bucket_exists.return_value = True

    create_bucket_if_not_exists(bucket_name)

    mock_minio.bucket_exists.assert_called_once_with(bucket_name)

@patch('data_pipeline.minio_client.minio_client')
def test_upload_file(mock_minio):
    
    bucket_name = "test-bucket"
    file_path = "/path/to/testfile.txt"
    file_name = os.path.basename(file_path)
    
    upload_file(bucket_name, file_path)
    
    mock_minio.fput_object.assert_called_once_with(bucket_name, file_name, file_path)

@patch('data_pipeline.minio_client.minio_client')
def test_download_file(mock_minio):
    
    bucket_name = "test-bucket"
    file_name = "testfile.txt"
    local_file_path = "/local/path/to/testfile.txt"
    
    download_file(bucket_name, file_name, local_file_path)
    
    mock_minio.fget_object.assert_called_once_with(bucket_name, file_name, local_file_path)

if __name__ == "__main__":
    pytest.main()
