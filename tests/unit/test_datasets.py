# Copyright 2021 - 2026 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for Dataset user stories.

Spec: POST /datasets, GET /datasets, GET /datasets/{id},
PATCH /datasets/{id}, DELETE /datasets/{id}
"""

import pytest
from ghga_service_commons.utils.utc_dates import now_as_utc

from srs.core.models import ExperimentalMetadata, Publication, StudyStatus
from srs.ports.inbound.study_registry import StudyRegistryPort
from tests.conftest import USER_OTHER, USER_STEWARD, USER_SUBMITTER
from tests.fixtures.examples import EXAMPLES

E = EXAMPLES


# ── helpers ──────────────────────────────────────────────────────


async def _setup(controller):
    """Create a study + DAC + DAP and return (study_id, dap_id)."""
    study = await controller.create_study(
        **E["studies"]["minimal"], created_by=USER_SUBMITTER,
    )
    await controller.create_dac(**E["dacs"]["default"])
    await controller.create_dap(**E["daps"]["default"])
    return study.id, "DAP-1"


async def _persist_study(controller, study_id, metadata_dao, publication_dao):
    await metadata_dao.insert(
        ExperimentalMetadata(
            id=study_id, metadata={}, submitted=now_as_utc()
        )
    )
    await publication_dao.insert(
        Publication(
            **E["publications"]["pub1"],
            study_id=study_id,
            created=now_as_utc(),
        )
    )
    await controller.update_study(
        study_id=study_id,
        status=StudyStatus.PERSISTED,
        approved_by=USER_STEWARD,
    )


# ── POST /datasets ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_dataset_generates_pid(controller):
    """A dataset must receive an auto-generated PID starting with GHGAD."""
    sid, dap_id = await _setup(controller)
    ds = await controller.create_dataset(
        **E["datasets"]["minimal"], study_id=sid, dap_id=dap_id,
    )
    assert ds.id.startswith("GHGAD")
    assert len(ds.id) == 19


@pytest.mark.asyncio
async def test_create_dataset_registers_accession(controller, accession_dao):
    """Creating a dataset must register an Accession entry."""
    sid, dap_id = await _setup(controller)
    ds = await controller.create_dataset(
        **E["datasets"]["minimal"], study_id=sid, dap_id=dap_id,
    )
    acc = await accession_dao.get_by_id(ds.id)
    assert acc.type == "DATASET"


@pytest.mark.asyncio
async def test_create_dataset_study_not_found(controller):
    """Creating a dataset for a non-existent study must raise StudyNotFoundError."""
    await controller.create_dac(**E["dacs"]["default"])
    await controller.create_dap(**E["daps"]["default"])
    with pytest.raises(StudyRegistryPort.StudyNotFoundError):
        await controller.create_dataset(
            **E["datasets"]["minimal"], study_id="NONEXIST", dap_id="DAP-1",
        )


@pytest.mark.asyncio
async def test_create_dataset_study_not_pending(
    controller, metadata_dao, publication_dao
):
    """Creating a dataset when the study is not PENDING must raise StatusConflictError."""
    sid, dap_id = await _setup(controller)
    await _persist_study(controller, sid, metadata_dao, publication_dao)

    with pytest.raises(StudyRegistryPort.StatusConflictError):
        await controller.create_dataset(
            **E["datasets"]["minimal"], study_id=sid, dap_id=dap_id,
        )


@pytest.mark.asyncio
async def test_create_dataset_dap_not_found(controller):
    """Creating a dataset with a non-existent DAP must raise DapNotFoundError."""
    study = await controller.create_study(
        **E["studies"]["minimal"], created_by=USER_SUBMITTER,
    )
    with pytest.raises(StudyRegistryPort.DapNotFoundError):
        await controller.create_dataset(
            **E["datasets"]["minimal"], study_id=study.id, dap_id="NONEXIST",
        )


@pytest.mark.asyncio
async def test_create_dataset_duplicate_files(controller):
    """Creating a dataset with duplicate file aliases must raise ValidationError."""
    sid, dap_id = await _setup(controller)
    with pytest.raises(StudyRegistryPort.ValidationError):
        await controller.create_dataset(
            **{**E["datasets"]["minimal"], "files": ["f1", "f1"]},
            study_id=sid,
            dap_id=dap_id,
        )


# ── GET /datasets ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_datasets_empty(controller):
    """With no datasets, the list must be empty."""
    result = await controller.get_datasets(
        user_id=USER_STEWARD, is_data_steward=True
    )
    assert result == []


@pytest.mark.asyncio
async def test_get_datasets_filter_by_study(controller):
    """Filtering by study_id must only return datasets of that study."""
    sid, dap_id = await _setup(controller)
    await controller.create_dataset(
        **E["datasets"]["minimal"], study_id=sid, dap_id=dap_id,
    )
    result = await controller.get_datasets(
        study_id=sid, user_id=USER_SUBMITTER
    )
    assert len(result) == 1


@pytest.mark.asyncio
async def test_get_datasets_access_control(controller):
    """Non-steward users must only see datasets of accessible studies."""
    sid, dap_id = await _setup(controller)
    await controller.create_dataset(
        **E["datasets"]["minimal"], study_id=sid, dap_id=dap_id,
    )

    # Submitter can see it
    result = await controller.get_datasets(user_id=USER_SUBMITTER)
    assert len(result) == 1

    # Other user cannot
    result = await controller.get_datasets(user_id=USER_OTHER)
    assert len(result) == 0


@pytest.mark.asyncio
async def test_get_datasets_text_filter(controller):
    """Text filter must match partial text in title or description."""
    sid, dap_id = await _setup(controller)
    await controller.create_dataset(
        **{**E["datasets"]["minimal"], "title": "Genomic Data", "description": "WGS files"},
        study_id=sid, dap_id=dap_id,
    )
    await controller.create_dataset(
        **{**E["datasets"]["minimal"], "title": "Clinical Data", "description": "Phenotype info"},
        study_id=sid, dap_id=dap_id,
    )
    result = await controller.get_datasets(
        text="Genomic", user_id=USER_SUBMITTER
    )
    assert len(result) == 1


# ── GET /datasets/{id} ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_dataset_by_id(controller):
    """Getting a dataset by ID must return it."""
    sid, dap_id = await _setup(controller)
    ds = await controller.create_dataset(
        **E["datasets"]["with_files"], study_id=sid, dap_id=dap_id,
    )
    fetched = await controller.get_dataset(
        dataset_id=ds.id, user_id=USER_SUBMITTER
    )
    assert fetched.files == ["f1"]


@pytest.mark.asyncio
async def test_get_dataset_not_found(controller):
    """Getting a non-existent dataset must raise DatasetNotFoundError."""
    with pytest.raises(StudyRegistryPort.DatasetNotFoundError):
        await controller.get_dataset(
            dataset_id="NONEXIST", user_id=USER_SUBMITTER
        )


@pytest.mark.asyncio
async def test_get_dataset_access_denied(controller):
    """Unauthorized users must get AccessDeniedError."""
    sid, dap_id = await _setup(controller)
    ds = await controller.create_dataset(
        **E["datasets"]["minimal"], study_id=sid, dap_id=dap_id,
    )
    with pytest.raises(StudyRegistryPort.AccessDeniedError):
        await controller.get_dataset(
            dataset_id=ds.id, user_id=USER_OTHER
        )


# ── PATCH /datasets/{id} ────────────────────────────────────────


@pytest.mark.asyncio
async def test_update_dataset_dap(controller):
    """Updating a dataset's DAP assignment must persist the change."""
    sid, dap_id = await _setup(controller)
    ds = await controller.create_dataset(
        **E["datasets"]["minimal"], study_id=sid, dap_id=dap_id,
    )
    await controller.create_dap(**E["daps"]["second"])
    await controller.update_dataset(dataset_id=ds.id, dap_id="DAP-2")
    updated = await controller.get_dataset(
        dataset_id=ds.id, user_id=USER_SUBMITTER
    )
    assert updated.dap_id == "DAP-2"


@pytest.mark.asyncio
async def test_update_dataset_dap_even_when_persisted(
    controller, metadata_dao, publication_dao
):
    """The DAP assignment can be updated even when the study is PERSISTED."""
    sid, dap_id = await _setup(controller)
    ds = await controller.create_dataset(
        **E["datasets"]["minimal"], study_id=sid, dap_id=dap_id,
    )
    await _persist_study(controller, sid, metadata_dao, publication_dao)

    await controller.create_dap(**E["daps"]["second"])
    await controller.update_dataset(dataset_id=ds.id, dap_id="DAP-2")
    updated = await controller.get_dataset(
        dataset_id=ds.id, user_id=USER_SUBMITTER
    )
    assert updated.dap_id == "DAP-2"


@pytest.mark.asyncio
async def test_update_dataset_dap_not_found(controller):
    """Updating to a non-existent DAP must raise DapNotFoundError."""
    sid, dap_id = await _setup(controller)
    ds = await controller.create_dataset(
        **E["datasets"]["minimal"], study_id=sid, dap_id=dap_id,
    )
    with pytest.raises(StudyRegistryPort.DapNotFoundError):
        await controller.update_dataset(dataset_id=ds.id, dap_id="NONEXIST")


@pytest.mark.asyncio
async def test_update_dataset_not_found(controller):
    """Updating a non-existent dataset must raise DatasetNotFoundError."""
    with pytest.raises(StudyRegistryPort.DatasetNotFoundError):
        await controller.update_dataset(
            dataset_id="NONEXIST", dap_id="DAP-1"
        )


# ── DELETE /datasets/{id} ───────────────────────────────────────


@pytest.mark.asyncio
async def test_delete_dataset(controller, dataset_dao, accession_dao):
    """Deleting a dataset must remove both the dataset and its accession."""
    sid, dap_id = await _setup(controller)
    ds = await controller.create_dataset(
        **E["datasets"]["minimal"], study_id=sid, dap_id=dap_id,
    )
    await controller.delete_dataset(dataset_id=ds.id)
    assert ds.id not in dataset_dao.resources
    assert ds.id not in accession_dao.resources


@pytest.mark.asyncio
async def test_delete_dataset_not_found(controller):
    """Deleting a non-existent dataset must raise DatasetNotFoundError."""
    with pytest.raises(StudyRegistryPort.DatasetNotFoundError):
        await controller.delete_dataset(dataset_id="NONEXIST")


@pytest.mark.asyncio
async def test_delete_dataset_study_not_pending(
    controller, metadata_dao, publication_dao
):
    """Deleting a dataset when the study is not PENDING must raise StatusConflictError."""
    sid, dap_id = await _setup(controller)
    ds = await controller.create_dataset(
        **E["datasets"]["minimal"], study_id=sid, dap_id=dap_id,
    )
    await _persist_study(controller, sid, metadata_dao, publication_dao)

    with pytest.raises(StudyRegistryPort.StatusConflictError):
        await controller.delete_dataset(dataset_id=ds.id)
