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

"""Tests for ExperimentalMetadata user stories.

Spec: POST (upsert) /metadata, GET /metadata/{study_id}, DELETE /metadata/{study_id}
"""

from datetime import datetime

import pytest
from ghga_service_commons.utils.utc_dates import now_as_utc

from srs.core.models import ExperimentalMetadata, Publication, StudyStatus
from srs.ports.inbound.study_registry import StudyRegistryPort
from tests.conftest import USER_STEWARD, USER_SUBMITTER
from tests.fixtures.examples import EXAMPLES

E = EXAMPLES


# ── helpers ──────────────────────────────────────────────────────


async def _create_pending_study(controller) -> str:
    """Create a PENDING study and return its ID."""
    study = await controller.studies.create_study(
        **E["studies"]["minimal"], created_by=USER_SUBMITTER,
    )
    return study.id


async def _persist_study(controller, study_id, metadata_dao, publication_dao):
    """Transition a study to PERSISTED by satisfying completeness."""
    await metadata_dao.insert(
        ExperimentalMetadata(
            id=study_id, metadata=E["metadata"]["empty"], submitted=now_as_utc()
        )
    )
    await publication_dao.insert(
        Publication(
            **E["publications"]["persisted"],
            study_id=study_id,
            created=now_as_utc(),
        )
    )
    await controller.studies.update_study(
        study_id=study_id,
        status=StudyStatus.PERSISTED,
        approved_by=USER_STEWARD,
    )


# ── POST /metadata (upsert) ─────────────────────────────────────


@pytest.mark.asyncio
async def test_upsert_metadata_create(controller):
    """Upserting metadata for a PENDING study must create the EM."""
    sid = await _create_pending_study(controller)
    await controller.metadata.upsert_metadata(
        study_id=sid, metadata=E["metadata"]["with_file"]
    )
    em = await controller.metadata.get_metadata(study_id=sid)
    assert em.metadata == E["metadata"]["with_file"]


@pytest.mark.asyncio
async def test_upsert_metadata_update(controller):
    """Upserting twice must overwrite the existing EM."""
    sid = await _create_pending_study(controller)
    await controller.metadata.upsert_metadata(study_id=sid, metadata={"v": 1})
    await controller.metadata.upsert_metadata(study_id=sid, metadata={"v": 2})
    em = await controller.metadata.get_metadata(study_id=sid)
    assert em.metadata == {"v": 2}


@pytest.mark.asyncio
async def test_upsert_metadata_study_not_found(controller):
    """Upserting metadata for a non-existent study must raise StudyNotFoundError."""
    with pytest.raises(StudyRegistryPort.StudyNotFoundError):
        await controller.metadata.upsert_metadata(
            study_id="NONEXIST", metadata={}
        )


@pytest.mark.asyncio
async def test_upsert_metadata_study_not_pending(
    controller, metadata_dao, publication_dao
):
    """Upserting metadata for a non-PENDING study must raise StatusConflictError."""
    sid = await _create_pending_study(controller)
    await _persist_study(controller, sid, metadata_dao, publication_dao)

    with pytest.raises(StudyRegistryPort.StatusConflictError):
        await controller.metadata.upsert_metadata(
            study_id=sid, metadata={"new": True}
        )


# ── GET /metadata/{study_id} ────────────────────────────────────


@pytest.mark.asyncio
async def test_get_metadata(controller):
    """Getting metadata must return the EM for the study."""
    sid = await _create_pending_study(controller)
    await controller.metadata.upsert_metadata(
        study_id=sid, metadata=E["metadata"]["with_file"]
    )
    em = await controller.metadata.get_metadata(study_id=sid)
    assert em.id == sid
    assert isinstance(em.submitted, datetime)


@pytest.mark.asyncio
async def test_get_metadata_not_found(controller):
    """Getting metadata for a study without EM must raise MetadataNotFoundError."""
    sid = await _create_pending_study(controller)
    with pytest.raises(StudyRegistryPort.MetadataNotFoundError):
        await controller.metadata.get_metadata(study_id=sid)


# ── DELETE /metadata/{study_id} ─────────────────────────────────


@pytest.mark.asyncio
async def test_delete_metadata(controller, metadata_dao):
    """Deleting metadata for a PENDING study must remove it."""
    sid = await _create_pending_study(controller)
    await controller.metadata.upsert_metadata(study_id=sid, metadata={"x": 1})
    await controller.metadata.delete_metadata(study_id=sid)
    assert sid not in metadata_dao.resources


@pytest.mark.asyncio
async def test_delete_metadata_study_not_pending(
    controller, metadata_dao, publication_dao
):
    """Deleting metadata for a non-PENDING study must raise StatusConflictError."""
    sid = await _create_pending_study(controller)
    await _persist_study(controller, sid, metadata_dao, publication_dao)

    with pytest.raises(StudyRegistryPort.StatusConflictError):
        await controller.metadata.delete_metadata(study_id=sid)


@pytest.mark.asyncio
async def test_delete_metadata_not_found(controller):
    """Deleting metadata that doesn't exist must raise MetadataNotFoundError."""
    sid = await _create_pending_study(controller)
    with pytest.raises(StudyRegistryPort.MetadataNotFoundError):
        await controller.metadata.delete_metadata(study_id=sid)
