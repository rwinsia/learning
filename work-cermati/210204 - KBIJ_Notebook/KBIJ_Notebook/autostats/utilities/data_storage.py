from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import os

class DataStorage:
    BUCKET_NAME = 'cermati-indodana-business-intelligence'
    NOTEBOOKS_FOLDER = "notebooks"
    VERSION_LATEST = "LATEST"

    def __init__(self, path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path

    @staticmethod
    def list_files():
        """List out file existed in current directory

        Return
        ---------
        list of filename
        """
        directory = DataStorage._get_directory()
        storage_client = storage.Client(project="indodana-bi-prod")
        bucket = storage_client.bucket(DataStorage.BUCKET_NAME)
        blobs = bucket.list_blobs(prefix=directory)
        blobnames = [blob.name for blob in list(blobs)]
        filenames = set([name.split('__')[0].split('/')[-1] for name in blobnames])
        return list(filenames)
        
    @staticmethod
    def read_csv(filename, version="LATEST"):
        if os.path.exists(filename) == False:
            directory = DataStorage._get_directory()
            source_filename = directory + "/" + filename.split(".")[0]
            DataStorage.download_with_versioning(filename,
                source_filename, version=version)
        return pd.read_csv(filename)

    @staticmethod
    def to_gcs(df, filename, index=False):
        df.to_csv(filename, index=index)
        directory = DataStorage._get_directory()
        blob_name = directory + "/" + filename.split(".")[0]
        DataStorage.upload_with_versioning(blob_name, filename)

    @staticmethod
    def to_csv(df, filename, index=False):
        DataStorage.to_gcs(df, filename, index=index)

    def _get_directory():
        full_path = os.getcwd()
        # for windows
        full_path = full_path.replace("\\","/")
        paths = full_path.split("/")
        pancake_index = paths.index('automatic-pancake')
        paths = paths[pancake_index+1:]
        return "/".join(paths)


    @staticmethod
    def _get_blob_name(parent_folder, organization, filename):
        if " " in filename or " " in organization or " " in parent_folder:
            raise ValueError("Whitespace detected in destination naming.")
        parent_folder = parent_folder.strip("/").lower()
        organization = organization.strip("/").lower()
        filename = filename.lower()
        blob_name = "/".join([parent_folder, organization, filename])
        return blob_name

    @staticmethod
    def _get_version_information(destination_blob_name):
        storage_client = storage.Client(project="indodana-bi-prod")
        blobs = storage_client.list_blobs(DataStorage.BUCKET_NAME)
        version_info = { "latest_version": 0 }
        for blob in blobs:
            blob_name = blob.name
            blob_array = blob_name.split("__")
            if len(blob_array) > 1:
                filename = blob_array[0]
                version = int(blob_array[1])
                if destination_blob_name == filename:
                    if version > version_info["latest_version"]:
                        version_info["latest_version"] = version
                        version_info["latest_blob_name"] = blob_name
        next_version = version_info["latest_version"] + 1
        version_info["next_version"] = next_version
        version_info["next_blob_name"] = "__".join([destination_blob_name, str(next_version)])
        return version_info

    @staticmethod
    def upload_with_versioning(destination_blob_name, source_filename):
        storage_client = storage.Client(project="indodana-bi-prod")
        bucket = storage_client.bucket(DataStorage.BUCKET_NAME)
        version_info = DataStorage._get_version_information(destination_blob_name)
        version_name = version_info["next_blob_name"]
        blob = bucket.blob(version_name)

        blob.upload_from_filename(source_filename)

        print(
            "File {} version {} uploaded to {}.".format(
                source_filename, version_info["next_version"], destination_blob_name
            )
        )


    @staticmethod
    def download(blob_name, destination_filename):
        storage_client = storage.Client(project="indodana-bi-prod")
        bucket = storage_client.bucket(DataStorage.BUCKET_NAME)
        blob = bucket.blob(blob_name)
        blob.download_to_filename(destination_filename)
        print(
            "Blob {} downloaded to {}.".format(
                blob_name, destination_filename
            )
        )

    @staticmethod
    def download_with_versioning(destination_filename, blob_name, version="LATEST"):
        storage_client = storage.Client(project="indodana-bi-prod")
        bucket = storage_client.bucket(DataStorage.BUCKET_NAME)
        version_info = DataStorage._get_version_information(blob_name)

        if version == DataStorage.VERSION_LATEST:
            version_name = version_info["latest_blob_name"]
        elif type(version) == int and version > 0:
            version_name = "__".join([blob_name, str(version)])
        else:
            raise ValueError("Version must be a positive integer or 'LATEST'")

        blob = bucket.blob(version_name)
        blob.download_to_filename(destination_filename)
        print(
            "Blob {} version {} downloaded to {}.".format(
                blob_name, version, destination_filename
            )
        )

    @staticmethod
    def upload_notebooks_data(source_filename, organization, destination_filename):
        """Uploads a file to the bucket.

        Parameters
        ---------
        organization: path/to/notebook starting from 'automatic-pancakes'
        example: /bank-mutation
        filename: bank-mutation-data

        """
        destination_blob_name = DataStorage._get_blob_name(
                DataStorage.NOTEBOOKS_FOLDER,
                organization,
                destination_filename)
        DataStorage.upload_with_versioning(destination_blob_name, source_filename)

    @staticmethod
    def download_notebooks_data(destination_filename, organization, source_filename,
                                version="LATEST"):
        """
        Downloads notebooks data from storage.

        Parameters
        ---------
        destination_filename: path/to/local folder filename to save the file.
        organization: name of the organization
        source_filename: filename
        version: integer to get specific version OR "LATEST" to get latest version.


        """
        blob_name = DataStorage._get_blob_name(
                DataStorage.NOTEBOOKS_FOLDER,
                organization,
                source_filename)
        DataStorage.download_with_versioning(destination_filename, blob_name, version=version)
