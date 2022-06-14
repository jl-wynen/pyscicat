from typing import Callable, Sequence

from ..client import ScicatClient
from ..model import Dataset, DataFile

try:
    from typing import Protocol as _Protocol

    class Upload(_Protocol):
        def __call__(self, path: str, files: Sequence[DataFile]) -> None:
            pass

        def revert(self, path: str, files: Sequence[DataFile]) -> None:
            pass

except ImportError:
    # Not exactly correct because it lacks revert,
    # but it should do until support for Python 3.7 is dropped.
    Upload = Callable[[str, Sequence[DataFile]], None]


def upload_dataset_and_files(
    client: ScicatClient, dataset: Dataset, files: Sequence[DataFile], upload: Upload
):
    upload(dataset.sourceFolder, files)
    try:
        client.replace_dataset(dataset)
    except Exception:
        upload.revert(dataset.sourceFolder, files)
        raise
