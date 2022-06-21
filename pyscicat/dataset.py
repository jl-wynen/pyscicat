from __future__ import annotations
from collections.abc import MutableMapping
from datetime import datetime, timezone
import os
from pathlib import Path
from typing import Optional, Union

from .model import DerivedDataset, DataFile, Datablock, OrigDatablock, Ownable


def _make_model_accessor(field_name: str, model_name: str):
    return property(lambda self: getattr(getattr(self, model_name), field_name),
                    lambda self, val: setattr(getattr(self, model_name), field_name, val))


def _wrap_model(model, model_name: str):
    def impl(cls):
        for field in model.__fields__:
            setattr(cls, field, _make_model_accessor(field_name=field, model_name=model_name))
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
    return datetime.fromtimestamp(st.st_mtime).astimezone(timezone.utc).isoformat(timespec='seconds')


class DatablockRENAMEME:
    def __init__(self, *, datablock: Optional[Datablock], orig_datablock: Optional[OrigDatablock]):
        assert (datablock is None) ^ (orig_datablock is None)
        self._datablock = datablock
        self._orig_datablock = orig_datablock

    def get_either(self) -> Union[Datablock, OrigDatablock]:
        return self._orig_datablock if self._datablock is None else self._datablock

    # block = OrigDatablock(size=size,
    #                       dataFileList=[DataFile(path=str(filename), size=size, time=_creation_time_str(st))],
    #                       datasetId=dataset.pid,
    #                       **ownable.dict())
    # return File(datablock=None,
    #             orig_datablock=block)


class File:
    def __init__(self, *, data_file: DataFile, local_path: Union[str, Path]):
        self._data_file = data_file
        self._local_path = Path(local_path)

    @property
    def local_path(self) -> Optional[Path]:
        return self._local_path

    def have_local_copy(self) -> bool:
        return self._local_path is not None

    @classmethod
    def from_local_file(cls, filename: Union[str, Path]) -> File:
        # TODO checksum once supported by the model
        st = os.stat(filename)
        size = st.st_size
        return File(data_file=DataFile(path='>PLACEHOLDER<', size=size, time=_creation_time_str(st)),
                    local_path=filename)

    @classmethod
    def from_scicat(cls):
        # TODO
        ...


@_wrap_model(DerivedDataset, 'scicat_model')
class DatasetRENAMEME:
    def __init__(self, ds: DerivedDataset):
        self._scicat_model = ds
        self._files = []

    @property
    def scicat_model(self) -> DerivedDataset:
        return self._scicat_model

    @property
    def meta(self):
        return ScientificMetadata(self.scicat_model)
