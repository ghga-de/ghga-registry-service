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

import pytest
import pytest_asyncio
from ghga_service_commons.utils.utc_dates import now_as_utc

from datetime import datetime

from srs.core.models import ExperimentalMetadata, Publication, StudyStatus
from srs.ports.inbound.study_registry import StudyRegistryPort
from tests.conftest import USER_STEWARD, USER_SUBMITTER
from tests.fixtures.examples import EXAMPLES

E = EXAMPLES


# ── fixtures ─────────────────────────────────────────────────────


@pytest.fixture()
def persist_study(controller, metadata_dao, publication_dao):
    """Return an async callable that transitions a study to PERSISTED state."""
    async def _persist(study_id: str) -> None:
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
    return _persist


# ── POST /metadata (upsert) ─────────────────────────────────────


@pytest.mark.asyncio
async def test_upsert_metadata_create(controller, pending_study_id):
    """Upserting metadata for a PENDING study must create the EM."""
    sid = pending_study_id
    await controller.metadata.upsert_metadata(
        study_id=sid, metadata=E["metadata"]["with_file"]
    )
    em = await controller.metadata.get_metadata(study_id=sid)
    assert em.metadata == E["metadata"]["with_file"]


@pytest.mark.asyncio
async def test_upsert_metadata_update(controller, pending_study_id):
    """Upserting twice must overwrite the existing EM."""
    sid = pending_study_id
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
    controller, pending_study_id, persist_study
):
    """Upserting metadata for a non-PENDING study must raise StatusConflictError."""
    sid = pending_study_id
    await persist_study(sid)

    with pytest.raises(StudyRegistryPort.StatusConflictError):
        await controller.metadata.upsert_metadata(
            study_id=sid, metadata={"new": True}
        )


# ── GET /metadata/{study_id} ────────────────────────────────────


@pytest.mark.asyncio
async def test_get_metadata(controller, pending_study_id):
    """Getting metadata must return the EM for the study."""
    sid = pending_study_id
    await controller.metadata.upsert_metadata(
        study_id=sid, metadata=E["metadata"]["with_file"]
    )
    em = await controller.metadata.get_metadata(study_id=sid)
    assert em.id == sid
    assert isinstance(em.submitted, datetime)


@pytest.mark.asyncio
async def test_get_metadata_not_found(controller, pending_study_id):
    """Getting metadata for a study without EM must raise MetadataNotFoundError."""
    sid = pending_study_id
    with pytest.raises(StudyRegistryPort.MetadataNotFoundError):
        await controller.metadata.get_metadata(study_id=sid)


# ── DELETE /metadata/{study_id} ─────────────────────────────────


@pytest.mark.asyncio
async def test_delete_metadata(controller, pending_study_id, metadata_dao):
    """Deleting metadata for a PENDING study must remove it."""
    sid = pending_study_id
    await controller.metadata.upsert_metadata(study_id=sid, metadata={"x": 1})
    await controller.metadata.delete_metadata(study_id=sid)
    assert sid not in metadata_dao.resources


@pytest.mark.asyncio
async def test_delete_metadata_study_not_pending(
    controller, pending_study_id, persist_study
):
    """Deleting metadata for a non-PENDING study must raise StatusConflictError."""
    sid = pending_study_id
    await persist_study(sid)

    with pytest.raises(StudyRegistryPort.StatusConflictError):
        await controller.metadata.delete_metadata(study_id=sid)


@pytest.mark.asyncio
async def test_delete_metadata_not_found(controller, pending_study_id):
    """Deleting metadata that doesn't exist must raise MetadataNotFoundError."""
    sid = pending_study_id
    with pytest.raises(StudyRegistryPort.MetadataNotFoundError):
        await controller.metadata.delete_metadata(study_id=sid)
