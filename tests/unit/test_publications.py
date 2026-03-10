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

"""Tests for Publication user stories.

Spec: POST /publications, GET /publications, GET /publications/{id},
DELETE /publications/{id}
"""

import pytest

from srs.ports.inbound.study_registry import StudyRegistryPort
from tests.conftest import USER_OTHER, USER_STEWARD, USER_SUBMITTER
from tests.fixtures.examples import EXAMPLES

E = EXAMPLES


# ── fixtures ─────────────────────────────────────────────────────


@pytest.fixture()
def create_pub(controller):
    """Return an async callable that creates a publication for a study."""
    async def _create(study_id: str, title: str = "P"):
        return await controller.publications.create_publication(
            data={**E["publications"]["minimal"], "title": title, "study_id": study_id},
        )
    return _create


# ── POST /publications ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_publication_generates_pid(pending_study_id, create_pub):
    """A publication must receive an auto-generated PID starting with GHGAU."""
    pub = await create_pub(pending_study_id)
    assert pub.id.startswith("GHGAU")
    assert len(pub.id) == 19


@pytest.mark.asyncio
async def test_create_publication_registers_accession(
    pending_study_id, create_pub, accession_dao
):
    """Creating a publication must also register an Accession entry."""
    pub = await create_pub(pending_study_id)
    acc = await accession_dao.get_by_id(pub.id)
    assert acc.type == "PUBLICATION"


@pytest.mark.asyncio
async def test_create_publication_links_to_study(pending_study_id, create_pub):
    """The publication must reference the correct study."""
    pub = await create_pub(pending_study_id)
    assert pub.study_id == pending_study_id


@pytest.mark.asyncio
async def test_create_publication_study_not_found(create_pub):
    """Publishing under a non-existent study must raise StudyNotFoundError."""
    with pytest.raises(StudyRegistryPort.StudyNotFoundError):
        await create_pub("NONEXIST")


@pytest.mark.asyncio
async def test_create_publication_study_not_pending(
    controller, pending_study_id, create_pub, metadata_dao, publication_dao
):
    """Publications cannot be added to a non-PENDING study."""
    from ghga_service_commons.utils.utc_dates import now_as_utc

    from srs.core.models import ExperimentalMetadata, Publication, StudyStatus

    sid = pending_study_id
    await metadata_dao.insert(
        ExperimentalMetadata(
            id=sid, metadata={}, submitted=now_as_utc()
        )
    )
    await publication_dao.insert(
        Publication(
            **E["publications"]["pub1"],
            study_id=sid,
            created=now_as_utc(),
        )
    )
    await controller.studies.update_study(
        study_id=sid,
        status=StudyStatus.PERSISTED,
        approved_by=USER_STEWARD,
    )
    with pytest.raises(StudyRegistryPort.StatusConflictError):
        await create_pub(sid, title="New")


# ── GET /publications ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_publications_filters_by_year(controller, pending_study_id):
    """Filtering by year must only return matching publications."""
    sid = pending_study_id
    await controller.publications.create_publication(
        data={**E["publications"]["year_2024"], "study_id": sid},
    )
    await controller.publications.create_publication(
        data={**E["publications"]["year_2025"], "study_id": sid},
    )
    result = await controller.publications.get_publications(
        year=2024, user_id=USER_SUBMITTER
    )
    assert len(result) == 1
    assert result[0].year == 2024


@pytest.mark.asyncio
async def test_get_publications_text_filter(controller, pending_study_id):
    """Text filter must match partial text in title, authors, etc."""
    sid = pending_study_id
    await controller.publications.create_publication(
        data={**E["publications"]["cancer"], "study_id": sid},
    )
    await controller.publications.create_publication(
        data={**E["publications"]["heart"], "study_id": sid},
    )

    result = await controller.publications.get_publications(
        text="cancer", user_id=USER_SUBMITTER
    )
    assert len(result) == 1
    assert "Cancer" in result[0].title


@pytest.mark.asyncio
async def test_get_publications_access_control(controller, pending_study_id, create_pub):
    """Non-steward users must only see publications of accessible studies."""
    sid = pending_study_id
    await create_pub(sid)

    # Submitter can see it
    result = await controller.publications.get_publications(user_id=USER_SUBMITTER)
    assert len(result) == 1

    # Other user cannot
    result = await controller.publications.get_publications(user_id=USER_OTHER)
    assert len(result) == 0

    # Steward can
    result = await controller.publications.get_publications(
        user_id=USER_STEWARD, is_data_steward=True
    )
    assert len(result) == 1


# ── GET /publications/{id} ──────────────────────────────────────


@pytest.mark.asyncio
async def test_get_publication_by_id(controller, pending_study_id, create_pub):
    """Getting a publication by ID must return it."""
    sid = pending_study_id
    pub = await create_pub(sid)
    fetched = await controller.publications.get_publication(
        publication_id=pub.id, user_id=USER_SUBMITTER
    )
    assert fetched.title == "P"


@pytest.mark.asyncio
async def test_get_publication_not_found(controller):
    """Getting a non-existent publication must raise PublicationNotFoundError."""
    with pytest.raises(StudyRegistryPort.PublicationNotFoundError):
        await controller.publications.get_publication(
            publication_id="NONEXIST", user_id=USER_SUBMITTER
        )


@pytest.mark.asyncio
async def test_get_publication_access_denied(controller, pending_study_id, create_pub):
    """Unauthorized users must get AccessDeniedError."""
    sid = pending_study_id
    pub = await create_pub(sid)
    with pytest.raises(StudyRegistryPort.AccessDeniedError):
        await controller.publications.get_publication(
            publication_id=pub.id, user_id=USER_OTHER
        )


# ── DELETE /publications/{id} ───────────────────────────────────


@pytest.mark.asyncio
async def test_delete_publication(controller, pending_study_id, create_pub, publication_dao, accession_dao):
    """Deleting a publication must remove both the publication and its accession."""
    sid = pending_study_id
    pub = await create_pub(sid)
    await controller.publications.delete_publication(publication_id=pub.id)
    assert pub.id not in publication_dao.resources
    assert pub.id not in accession_dao.resources


@pytest.mark.asyncio
async def test_delete_publication_not_found(controller):
    """Deleting a non-existent publication must raise PublicationNotFoundError."""
    with pytest.raises(StudyRegistryPort.PublicationNotFoundError):
        await controller.publications.delete_publication(publication_id="NONEXIST")


@pytest.mark.asyncio
async def test_delete_publication_study_not_pending(
    controller, pending_study_id, create_pub, metadata_dao
):
    """Deleting a publication when the study is not PENDING must raise StatusConflictError."""
    from ghga_service_commons.utils.utc_dates import now_as_utc

    from srs.core.models import ExperimentalMetadata, Publication, StudyStatus

    sid = pending_study_id
    pub = await create_pub(sid, title="First")
    await metadata_dao.insert(
        ExperimentalMetadata(
            id=sid, metadata={}, submitted=now_as_utc()
        )
    )
    await controller.studies.update_study(
        study_id=sid,
        status=StudyStatus.PERSISTED,
        approved_by=USER_STEWARD,
    )
    with pytest.raises(StudyRegistryPort.StatusConflictError):
        await controller.publications.delete_publication(publication_id=pub.id)
