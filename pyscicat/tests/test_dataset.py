import pytest
from rich import print

from ..dataset import DatasetRENAMEME, File
from ..model import DatasetType, DataFile, DerivedDataset, Ownable


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


def test_can_get_dataset_properties(derived_dataset):
    dset = DatasetRENAMEME(derived_dataset)
    assert dset.owner == "slartibartfast"
    assert dset.usedSoftware == ["PySciCat"]
    assert dset.datasetName is None


def test_can_set_dataset_properties(derived_dataset):
    dset = DatasetRENAMEME(derived_dataset)
    dset.owner = "marvin"
    dset.usedSoftware.append("Python")
    dset.datasetName = "Heart of Gold"
    assert dset.owner == "marvin"
    assert dset.usedSoftware == ["PySciCat", "Python"]
    assert dset.datasetName == "Heart of Gold"


def test_setting_dataset_properties_does_not_affect_other_attributes(derived_dataset):
    expected_fields = dict(derived_dataset)
    del expected_fields["owner"]
    dset = DatasetRENAMEME(derived_dataset)

    dset.owner = "marvin"
    fields = dict(dset.scicat_model)
    del fields["owner"]

    assert fields == expected_fields


def test_meta_behaves_like_dict(derived_dataset):
    dset = DatasetRENAMEME(derived_dataset)
    assert dset.scicat_model.scientificMetadata is None
    assert dset.meta == {}

    dset.meta['a'] = dict(value=3, unit='m')
    dset.meta['b'] = dict(value=-1.2, unit='s')
    assert dset.meta['a'] == dict(value=3, unit='m')
    assert dset.meta['b'] == dict(value=-1.2, unit='s')
    assert dset.scicat_model.scientificMetadata['a'] == dict(value=3, unit='m')
    assert dset.scicat_model.scientificMetadata['b'] == dict(value=-1.2, unit='s')

    assert len(dset.meta) == 2
    assert list(dset.meta) == ['a', 'b']
    assert list(dset.meta.keys()) == ['a', 'b']
    assert list(dset.meta.values()) == [dict(value=3, unit='m'), dict(value=-1.2, unit='s')]
    assert list(dset.meta.items()) == [('a', dict(value=3, unit='m')), ('b', dict(value=-1.2, unit='s'))]

    del dset.meta['a']
    assert 'a' not in dset.meta
    assert 'a' not in dset.scicat_model.scientificMetadata
    assert dset.meta['b'] == dict(value=-1.2, unit='s')
    assert dset.scicat_model.scientificMetadata['b'] == dict(value=-1.2, unit='s')


def test_main(derived_dataset, ownable):
    f = File.from_local_file('/home/jl/Work/pyscicat/README.md')
    print(f.local_path)

    assert False