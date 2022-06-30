from __future__ import annotations
from collections.abc import MutableMapping
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
from typing import List, Optional, Tuple, Union
from uuid import uuid4

from .client import ScicatClient, ScicatCommError
from .model import DerivedDataset, DataFile, DatasetType, RawDataset, OrigDatablock


def _make_model_accessor(field_name: str, model_name: str):
    return property(
        lambda self: getattr(getattr(self, model_name), field_name),
        lambda self, val: setattr(getattr(self, model_name), field_name, val),
    )


def _make_raising_accessor(field_name: str):
    # Prevent access to some attributes which are managed automatically.
    # Using a property that always raises here,
    # because that also prevents accidental assignments.
    def impl(_self):
        raise AttributeError(f"Attribute '{field_name}' is not accessible")

    return property(impl)


def _wrap_model(model, model_name: str, exclude: Tuple[str, ...]):
    def impl(cls):
        for field in model.__fields__:
            if field in exclude:
                setattr(cls, field, _make_raising_accessor(field))
            else:
                setattr(
                    cls,
                    field,
                    _make_model_accessor(field_name=field, model_name=model_name),
                )
        return cls

    return impl


# This wrapper is needed because DerivedDataset.scientificMetadata
# can be None in which case we need to write to the instance of
# DerivedDataset in __setitem__.
class ScientificMetadata(MutableMapping):
    def __init__(self, parent: DerivedDataset):
        self._parent = parent

    def _get_dict_or_empty(self) -> dict:
        return self._parent.scientificMetadata or {}

    def __getitem__(self, key: str):
        return self._get_dict_or_empty()[key]

    def __setitem__(self, key: str, value):
        if self._parent.scientificMetadata is None:
            self._parent.scientificMetadata = {key: value}
        else:
            self._parent.scientificMetadata[key] = value

    def __delitem__(self, key: str):
        del self._get_dict_or_empty()[key]

    def __iter__(self):
        return iter(self._get_dict_or_empty())

    def __len__(self):
        return len(self._get_dict_or_empty())

    def __repr__(self):
        return repr(self._get_dict_or_empty())


class File:
    def __init__(
        self,
        *,
        source_path: Union[str, Path],
        source_folder: Optional[Union[str, Path]],
        local_path: Optional[Union[str, Path]],
        model: Optional[DataFile],
    ):
        self._source_path = Path(source_path)  # relative to source folder
        self._source_folder = Path(source_folder) if source_folder is not None else None
        self._local_path = Path(local_path) if local_path is not None else None
        self._model = model

    @classmethod
    def from_local(
        cls, path: Union[str, Path], *, relative_to: Union[str, Path] = ""
    ) -> File:
        return File(
            source_path=Path(relative_to) / Path(path).name,
            source_folder=None,
            local_path=path,
            model=None,
        )

    @classmethod
    def from_scicat(cls, model: DataFile, sourceFolder: str) -> File:
        return File(
            source_path=model.path,
            source_folder=sourceFolder,
            local_path=None,
            model=model,
        )

    @property
    def source_path(self) -> Path:
        return self._source_path

    @property
    def source_folder(self) -> Optional[Path]:
        return self._source_folder

    @source_folder.setter
    def source_folder(self, value: Optional[Union[str, Path]]):
        self._source_folder = Path(value) if value is not None else None

    @property
    def remote_access_path(self) -> Optional[str]:
        return (
            None
            if self.source_folder is None
            else self._source_folder / self.source_path
        )

    @property
    def local_path(self) -> Optional[Path]:
        return self._local_path

    @property
    def model(self) -> Optional[DataFile]:
        return self._model

    # def have_local_copy(self) -> bool:
    #     return self._local_path is not None

    def with_model_from_local_file(self) -> File:
        # TODO checksum once supported by the model
        assert self._local_path is not None
        st = self._local_path.stat()
        return File(
            source_path=self._source_path,
            source_folder=self._source_folder,
            local_path=self._local_path,
            model=DataFile(
                path=str(self.source_path), size=st.st_size, time=_creation_time_str(st)
            ),
        )

    def __repr__(self):
        return (
            f"File(source_folder={self.source_folder}, source_path={self.source_path}, "
            f"local_path={self.local_path}, model={self.model!r})"
        )


# TODO handle orig vs non-orig datablocks
@_wrap_model(DerivedDataset, "model", exclude=("numberOfFiles", "size", "type"))
class DatasetRENAMEME:
    # TODO support RawDataset
    def __init__(
        self,
        *,
        model: DerivedDataset,
        files: List[File],
        datablock: Optional[OrigDatablock],
    ):
        self._model = model
        self._files = files
        self._datablock = datablock

    @classmethod
    def new(cls, model: Optional[DerivedDataset] = None, **kwargs) -> DatasetRENAMEME:
        model_dict = model.dict(exclude_none=True) if model is not None else {}
        model_dict.update(kwargs)
        model_dict.setdefault("sourceFolder", "<PLACEHOLDER>")
        return DatasetRENAMEME(
            model=DerivedDataset(**model_dict), files=[], datablock=None
        )

    @classmethod
    def from_scicat(cls, client: ScicatClient, pid: str) -> DatasetRENAMEME:
        model = _get_dataset_model(pid, client)
        dblock_json = _get_orig_datablock(pid, client)

        files = [
            File.from_scicat(DataFile(**file_json), model.sourceFolder)
            for file_json in dblock_json["dataFileList"]
        ]

        del dblock_json["dataFileList"]
        dblock = OrigDatablock(
            **dblock_json, dataFileList=[file.model for file in files]
        )

        return DatasetRENAMEME(model=model, files=files, datablock=dblock)

    @property
    def model(self) -> DerivedDataset:
        return self._model

    @property
    def datablock(self) -> Optional[OrigDatablock]:
        return self._datablock

    @property
    def meta(self):
        return ScientificMetadata(self.model)

    @property
    def dataset_type(self) -> DatasetType:
        return self._model.type

    @property
    def files(self) -> Tuple[File, ...]:
        return tuple(self._files)

    def add_files(self, *files: File):
        self._files.extend(files)

    def add_local_files(
        self, *paths: Union[str, Path], relative_to: Union[str, Path] = ""
    ):
        self.add_files(
            *(File.from_local(path, relative_to=relative_to) for path in paths)
        )

    def prepare_as_new(self) -> DatasetRENAMEME:
        files = list(map(File.with_model_from_local_file, self._files))
        total_size = sum(file.model.size for file in files)
        dataset_id = str(uuid4())
        datablock = OrigDatablock(
            size=total_size,
            dataFileList=[file.model for file in files],
            datasetId=dataset_id,
            ownerGroup=self.model.ownerGroup,
            accessGroups=self.model.accessGroups,
        )
        model = DerivedDataset(
            **{
                **self.model.dict(exclude_none=True),
                "pid": dataset_id,
                "numberOfFiles": len(files),
                "numberOfFilesArchived": None,
                "size": total_size,
                "sourceFolder": "<PLACEHOLDER>",
            }
        )
        return DatasetRENAMEME(model=model, files=files, datablock=datablock)

    def upload_new_dataset_now(self, client: ScicatClient, uploader_factory):
        if self._datablock is None:
            dset = self.prepare_as_new()
        else:
            dset = self
        uploader = uploader_factory(dataset_id=dset.pid)
        dset.sourceFolder = str(uploader.remote_upload_path)
        for file in dset.files:
            file.source_folder = dset.sourceFolder
            uploader.put(local=file.local_path, remote=file.remote_access_path)

        try:
            dataset_id = client.datasets_create(dset.model)["pid"]
        except ScicatCommError:
            for file in dset.files:
                uploader.revert_put(
                    local=file.local_path, remote=file.remote_access_path
                )
            raise

        dset.datablock.datasetId = dataset_id
        try:
            client.datasets_origdatablock_create(dset.datablock)
        except ScicatCommError as exc:
            raise RuntimeError(
                f"Failed to upload original datablocks for SciCat dataset {dset.pid}:"
                f"\n{exc.args}\nThe dataset and data files were successfully uploaded "
                "but are not linked with each other. Please fix the dataset manually!"
            )

        return dset


def _get_dataset_model(pid, client) -> Union[DerivedDataset, RawDataset]:
    dset_json = client.get_dataset_by_pid(pid)
    return (
        DerivedDataset(**dset_json)
        if dset_json["type"] == "derived"
        else RawDataset(**dset_json)
    )


def _get_orig_datablock(pid, client) -> dict:
    dblock_json = client.get_dataset_origdatablocks(pid)
    if len(dblock_json) != 1:
        raise NotImplementedError(
            f"Got {len(dblock_json)} original datablocks for dataset {pid} "
            "but only support for one is implemented."
        )
    return dblock_json[0]


def _ensure_source_folder(
    files: List[File], target_folder: Optional[Union[str, Path]] = None
) -> str:
    if target_folder is not None:
        for file in files:
            file.source_folder = Path(target_folder)
        return str(target_folder)

    source_folders = {
        str(file.source_folder) for file in files if file.source_folder is not None
    }
    if len(source_folders) >= 2:
        raise RuntimeError(
            "Cannot determine a unique source folder from files, "
            f"got: {source_folders}. "
            "Pass an explicit 'source_folder' argument to override."
        )
    if not source_folders:
        raise RuntimeError(
            "Cannot determine a source folder because no input files "
            "have a source folder set. "
            "Pass an explicit 'source_folder' argument to override."
        )
    return next(iter(source_folders))


def _creation_time_str(st: os.stat_result) -> str:
    """Return the time in UTC when a file was created.

    Uses modification time as SciCat only cares about the latest version of the file
    and not when it was first created on the local system.
    """
    # TODO is this correct on non-linux?
    # TODO is this correct if the file was created in a different timezone (DST)?
    return (
        datetime.fromtimestamp(st.st_mtime)
        .astimezone(timezone.utc)
        .isoformat(timespec="seconds")
    )


def md5sum(path: Union[str, Path]) -> str:
    md5 = hashlib.md5()
    # size based on http://git.savannah.gnu.org/gitweb/?p=coreutils.git;a=blob;f=src/ioblksize.h;h=ed2f4a9c4d77462f357353eb73ee4306c28b37f1;hb=HEAD#l23
    buffer = memoryview(bytearray(128 * 1024))
    with open(path, "rb", buffering=0) as file:
        for n in iter(lambda: file.readinto(buffer), 0):
            md5.update(buffer[:n])
    return md5.hexdigest()
