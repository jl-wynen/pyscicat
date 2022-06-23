from __future__ import annotations
from collections.abc import MutableMapping
from datetime import datetime, timezone
import os
from pathlib import Path
from typing import List, Optional, Tuple, Union
from uuid import uuid4

from .client import ScicatClient
from .model import DerivedDataset, DataFile, RawDataset, OrigDatablock


def _make_model_accessor(field_name: str, model_name: str):
    return property(
        lambda self: getattr(getattr(self, model_name), field_name),
        lambda self, val: setattr(getattr(self, model_name), field_name, val),
    )


def _wrap_model(model, model_name: str):
    def impl(cls):
        for field in model.__fields__:
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


def _creation_time_str(st: os.stat_result) -> str:
    """Return the time in UTC when a file was created.

    Uses modification time as SciCat only cares about the latest version of the file.
    """
    # TODO is this correct on non-linux?
    # TODO is this correct if the file was created in a different timezone (DST)?
    return (
        datetime.fromtimestamp(st.st_mtime)
        .astimezone(timezone.utc)
        .isoformat(timespec="seconds")
    )


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
# TODO do not expose certain attributes (size, numberOfFiles) and manage them internally
@_wrap_model(DerivedDataset, "model")
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
        return DatasetRENAMEME(
            model=DerivedDataset(**model_dict), files=[], datablock=None
        )

    @classmethod
    def from_scicat(cls, client: ScicatClient, pid: str) -> DatasetRENAMEME:
        dset_json = client.get_dataset_by_pid(pid)
        model = (
            DerivedDataset(**dset_json)
            if dset_json["type"] == "derived"
            else RawDataset(**dset_json)
        )

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
    def meta(self):
        return ScientificMetadata(self.model)

    @property
    def files(self) -> Tuple[File, ...]:
        return tuple(self._files)

    def add_file(self, file: File):
        self._files.append(file)

    def add_local_file(
        self, path: Union[str, Path], *, relative_to: Union[str, Path] = ""
    ) -> File:
        file = File.from_local(path, relative_to=relative_to)
        self.add_file(file)
        return file

    def finalize_model(
        self, *, source_folder: Optional[Union[str, Path]] = None
    ) -> DatasetRENAMEME:
        files = list(map(File.with_model_from_local_file, self._files))
        total_size = sum(file.model.size for file in files)
        # TODO might have datablock (w/ PID) in self
        dataset_id = uuid4()
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
                "numberOfFilesArchived": None,  # TODO related to orig / non-orig issue
                "size": total_size,
                "sourceFolder": _ensure_source_folder(files, source_folder),
            }
        )
        return DatasetRENAMEME(model=model, files=files, datablock=datablock)


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
