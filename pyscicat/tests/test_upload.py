import pytest
import requests_mock
from urllib.parse import urljoin

from ..client import ScicatClient
from ..model import DatasetType, DataFile, DerivedDataset, Ownable
from ..ingest.upload import upload_dataset_and_files


@pytest.fixture
def local_url():
    return "http://localhost:3000/api/v3/"


@pytest.fixture
def mock_request(local_url):
    with requests_mock.Mocker() as mock:
        mock.post(
            urljoin(local_url, "Users/login"),
            json={"id": "a_token"},
        )

        mock.post(
            urljoin(local_url, "DerivedDatasets/replaceOrCreate"),
            json={"pid": "1234-5678-abcd"},
        )

        yield mock


@pytest.fixture
def client(mock_request, local_url):
    return ScicatClient(
        base_url=local_url,
        username="Zaphod",
        password="heartofgold",
    )


@pytest.fixture
def ownable():
    return Ownable(ownerGroup="ownerGroup", accessGroups=["group1", "group2"])


@pytest.fixture
def derived_dataset(ownable):
    return DerivedDataset(
        contactEmail="slartibartfast@magrathea.org",
        creationTime="2022-06-14T12:34:56",
        owner="slartibartfast",
        sourceFolder="UPLOAD",
        type=DatasetType.derived,
        inputDatasets=[],
        usedSoftware=["PySciCat"],
        **ownable.dict()
    )


@pytest.fixture
def data_files():
    return [
        DataFile(path="file1.txt", size=100),
        DataFile(path="file2.nxs", size=54321),
    ]


class FakeUpload:
    def __init__(self):
        self.uploaded = []

    def __call__(self, path, files):
        assert path == "UPLOAD"
        self.uploaded.extend(files)

    def revert(self, path, files):
        assert path == "UPLOAD"
        for file in files:
            del self.uploaded[self.uploaded.index(file)]


def test_upload_creates_dataset(mock_request, client, derived_dataset):
    upload_dataset_and_files(client, derived_dataset, [], lambda *args, **kwargs: ...)
    assert mock_request.last_request.json() == derived_dataset.dict(exclude_none=True)


def test_upload_uploads_files_to_source_folder(client, derived_dataset, data_files):
    upload = FakeUpload()
    upload_dataset_and_files(client, derived_dataset, data_files, upload)
    assert upload.uploaded == data_files


def test_upload_does_not_create_dataset_if_file_upload_fails(
    mock_request, client, derived_dataset, data_files
):
    class RaisingUpload:
        def __call__(self, _path, _files):
            raise RuntimeError("Fake upload failure")

        def revert(self, _path, _files):
            raise RuntimeError("Fake upload revert failure")

    with pytest.raises(RuntimeError):
        upload_dataset_and_files(client, derived_dataset, data_files, RaisingUpload())

    assert all("Dataset" not in str(req) for req in mock_request.request_history)


def test_upload_cleans_up_files_if_dataset_ingestion_fails(
    local_url, mock_request, client, derived_dataset, data_files
):
    def fail_ingestion(_request, _context):
        raise RuntimeError("Ingestion failed")

    mock_request.reset()
    mock_request.post(
        urljoin(local_url, "DerivedDatasets/replaceOrCreate"), text=fail_ingestion
    )

    upload = FakeUpload()
    with pytest.raises(RuntimeError):
        upload_dataset_and_files(client, derived_dataset, data_files, upload)

    assert not upload.uploaded


def test_upload_creates_orig_data_blocks(mock_request, client, derived_dataset, data_files):
    upload_dataset_and_files(client, derived_dataset, data_files, FakeUpload())
    # TODO
    assert False
