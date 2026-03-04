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

"""Tests for Study user stories.

Spec: POST /studies, GET /studies, GET /studies/{id}, PATCH /studies/{id},
DELETE /studies/{id}
"""

import pytest

from srs.core.models import StudyStatus
from srs.ports.inbound.study_registry import StudyRegistryPort
from tests.conftest import USER_OTHER, USER_STEWARD, USER_SUBMITTER
from tests.fixtures.examples import EXAMPLES

E = EXAMPLES


# ── POST /studies ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_study_returns_study_with_pending_status(controller):
    """A newly created study must have status PENDING."""
    study = await controller.studies.create_study(
        **E["studies"]["default"], created_by=USER_SUBMITTER,
    )
    assert study.status == StudyStatus.PENDING


@pytest.mark.asyncio
async def test_create_study_generates_pid(controller):
    """The study must receive an auto-generated PID starting with GHGAS."""
    study = await controller.studies.create_study(
        **{**E["studies"]["default"], "types": [], "affiliations": []},
        created_by=USER_SUBMITTER,
    )
    assert study.id.startswith("GHGAS")
    assert len(study.id) == 19


@pytest.mark.asyncio
async def test_create_study_sets_users_to_creator(controller):
    """After creation, the users list must contain only the creator."""
    study = await controller.studies.create_study(
        **E["studies"]["with_desc"], created_by=USER_SUBMITTER,
    )
    assert study.users == [USER_SUBMITTER]


@pytest.mark.asyncio
async def test_create_study_registers_accession(controller, accession_dao):
    """Creating a study must also register an Accession entry."""
    study = await controller.studies.create_study(
        **E["studies"]["minimal"], created_by=USER_SUBMITTER,
    )
    acc = await accession_dao.get_by_id(study.id)
    assert acc.type == "STUDY"


# ── GET /studies ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_studies_returns_empty_list_initially(controller):
    """With no studies, the list must be empty."""
    result = await controller.studies.get_studies(
        user_id=USER_STEWARD, is_data_steward=True
    )
    assert result == []


@pytest.mark.asyncio
async def test_get_studies_filter_by_status(controller):
    """Filtering by status must only return matching studies."""
    await controller.studies.create_study(
        title="A", description="", types=[], affiliations=[],
        created_by=USER_SUBMITTER,
    )
    result = await controller.studies.get_studies(
        status=StudyStatus.PENDING,
        user_id=USER_SUBMITTER,
    )
    assert len(result) == 1

    result = await controller.studies.get_studies(
        status=StudyStatus.PERSISTED,
        user_id=USER_SUBMITTER,
    )
    assert len(result) == 0


@pytest.mark.asyncio
async def test_get_studies_text_filter(controller):
    """Text filter must match partial text in title, description, or affiliations."""
    await controller.studies.create_study(
        **E["studies"]["cancer_genomics"], created_by=USER_SUBMITTER,
    )
    await controller.studies.create_study(
        **E["studies"]["heart"], created_by=USER_SUBMITTER,
    )

    result = await controller.studies.get_studies(
        text="cancer", user_id=USER_SUBMITTER
    )
    assert len(result) == 1
    assert "Cancer" in result[0].title

    result = await controller.studies.get_studies(
        text="Heidelberg", user_id=USER_SUBMITTER
    )
    assert len(result) == 1


@pytest.mark.asyncio
async def test_get_studies_type_filter(controller):
    """Type filter must only return studies having the requested type."""
    await controller.studies.create_study(
        **E["studies"]["wgs_only"], created_by=USER_SUBMITTER,
    )
    await controller.studies.create_study(
        **E["studies"]["rna_seq_only"], created_by=USER_SUBMITTER,
    )

    result = await controller.studies.get_studies(
        study_type="WGS", user_id=USER_SUBMITTER
    )
    assert len(result) == 1
    assert result[0].title == "A"


@pytest.mark.asyncio
async def test_get_studies_pagination(controller):
    """Skip and limit must paginate results."""
    for i in range(5):
        await controller.studies.create_study(
            title=f"Study-{i}",
            description="",
            types=[],
            affiliations=[],
            created_by=USER_SUBMITTER,
        )
    result = await controller.studies.get_studies(
        skip=2, limit=2, user_id=USER_SUBMITTER
    )
    assert len(result) == 2


@pytest.mark.asyncio
async def test_get_studies_only_returns_accessible_studies(controller):
    """Non-steward users must only see studies they are listed in."""
    study = await controller.studies.create_study(
        **E["studies"]["private"], created_by=USER_SUBMITTER,
    )

    # Submitter can see it
    result = await controller.studies.get_studies(user_id=USER_SUBMITTER)
    assert len(result) == 1

    # Other user cannot
    result = await controller.studies.get_studies(user_id=USER_OTHER)
    assert len(result) == 0

    # Steward can see all
    result = await controller.studies.get_studies(
        user_id=USER_STEWARD, is_data_steward=True
    )
    assert len(result) == 1


@pytest.mark.asyncio
async def test_get_studies_strips_user_fields_for_non_steward(controller):
    """Non-steward callers must not see user-related fields."""
    await controller.studies.create_study(
        **E["studies"]["minimal"], created_by=USER_SUBMITTER,
    )
    result = await controller.studies.get_studies(user_id=USER_SUBMITTER)
    assert result[0].users is None  # stripped


# ── GET /studies/{id} ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_study_by_id(controller):
    """Getting a study by ID must return the study."""
    study = await controller.studies.create_study(
        **E["studies"]["with_desc"], created_by=USER_SUBMITTER,
    )
    fetched = await controller.studies.get_study(
        study_id=study.id,
        user_id=USER_SUBMITTER,
    )
    assert fetched.title == "Study"


@pytest.mark.asyncio
async def test_get_study_not_found(controller):
    """Getting a non-existent study must raise StudyNotFoundError."""
    with pytest.raises(StudyRegistryPort.StudyNotFoundError):
        await controller.studies.get_study(
            study_id="GHGAS_NONEXISTENT",
            user_id=USER_SUBMITTER,
        )


@pytest.mark.asyncio
async def test_get_study_access_denied_for_unauthorized_user(controller):
    """A user not in the users list must be denied access (403)."""
    study = await controller.studies.create_study(
        **E["studies"]["private"],
        created_by=USER_SUBMITTER,
    )
    with pytest.raises(StudyRegistryPort.AccessDeniedError):
        await controller.studies.get_study(
            study_id=study.id, user_id=USER_OTHER
        )


@pytest.mark.asyncio
async def test_get_study_steward_sees_user_fields(controller):
    """Data stewards must see the users field."""
    study = await controller.studies.create_study(
        **E["studies"]["minimal"], created_by=USER_SUBMITTER,
    )
    fetched = await controller.studies.get_study(
        study_id=study.id,
        user_id=USER_STEWARD,
        is_data_steward=True,
    )
    assert fetched.users == [USER_SUBMITTER]
    assert fetched.created_by == USER_SUBMITTER


@pytest.mark.asyncio
async def test_get_study_non_steward_gets_stripped(controller):
    """Non-steward users must not see user-related fields."""
    study = await controller.studies.create_study(
        **E["studies"]["minimal"], created_by=USER_SUBMITTER,
    )
    fetched = await controller.studies.get_study(
        study_id=study.id, user_id=USER_SUBMITTER
    )
    assert fetched.users is None
    assert fetched.approved_by is None


# ── PATCH /studies/{id} ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_update_study_status_pending_to_persisted(
    controller, study_dao, metadata_dao, publication_dao
):
    """Status must change from PENDING to PERSISTED after validation."""
    study = await controller.studies.create_study(
        **E["studies"]["minimal"], created_by=USER_SUBMITTER,
    )
    # Satisfy completeness: add EM and publication
    from ghga_service_commons.utils.utc_dates import now_as_utc

    from srs.core.models import ExperimentalMetadata, Publication

    await metadata_dao.insert(
        ExperimentalMetadata(
            id=study.id, metadata=E["metadata"]["empty"], submitted=now_as_utc()
        )
    )
    await publication_dao.insert(
        Publication(
            **E["publications"]["persisted"],
            study_id=study.id,
            created=now_as_utc(),
        )
    )

    await controller.studies.update_study(
        study_id=study.id,
        status=StudyStatus.PERSISTED,
        approved_by=USER_STEWARD,
    )
    updated = await study_dao.get_by_id(study.id)
    assert updated.status == StudyStatus.PERSISTED
    assert updated.approved_by == USER_STEWARD


@pytest.mark.asyncio
async def test_update_study_rejects_invalid_status_transition(controller):
    """Only PENDING -> PERSISTED is allowed; other transitions raise 409."""
    study = await controller.studies.create_study(
        **E["studies"]["minimal"], created_by=USER_SUBMITTER,
    )
    with pytest.raises(StudyRegistryPort.StatusConflictError):
        await controller.studies.update_study(
            study_id=study.id, status=StudyStatus.FROZEN
        )


@pytest.mark.asyncio
async def test_update_study_validates_completeness_on_status_change(controller):
    """Changing status to PERSISTED must validate completeness first."""
    study = await controller.studies.create_study(
        **E["studies"]["minimal"], created_by=USER_SUBMITTER,
    )
    # No EM or publication → should fail
    with pytest.raises(StudyRegistryPort.ValidationError):
        await controller.studies.update_study(
            study_id=study.id, status=StudyStatus.PERSISTED
        )


@pytest.mark.asyncio
async def test_update_study_not_found(controller):
    """Updating a non-existent study must raise StudyNotFoundError."""
    with pytest.raises(StudyRegistryPort.StudyNotFoundError):
        await controller.studies.update_study(
            study_id="NONEXIST", status=StudyStatus.PERSISTED
        )


# ── DELETE /studies/{id} ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_delete_study_cascade(
    controller, study_dao, metadata_dao, publication_dao, accession_dao
):
    """Deleting a PENDING study must cascade-delete EM, publications, accessions."""
    study = await controller.studies.create_study(
        **E["studies"]["minimal"], created_by=USER_SUBMITTER,
    )
    from ghga_service_commons.utils.utc_dates import now_as_utc

    from srs.core.models import ExperimentalMetadata

    await metadata_dao.insert(
        ExperimentalMetadata(
            id=study.id, metadata={}, submitted=now_as_utc()
        )
    )
    pub = await controller.publications.create_publication(
        **E["publications"]["minimal"], study_id=study.id,
    )

    await controller.studies.delete_study(study_id=study.id)

    assert study.id not in study_dao.resources
    assert study.id not in metadata_dao.resources
    assert pub.id not in publication_dao.resources
    # Study accession also deleted
    assert study.id not in accession_dao.resources


@pytest.mark.asyncio
async def test_delete_study_rejects_non_pending(
    controller, metadata_dao, publication_dao, study_dao
):
    """Deleting a non-PENDING study must raise StatusConflictError (409)."""
    study = await controller.studies.create_study(
        **E["studies"]["minimal"], created_by=USER_SUBMITTER,
    )
    # Force status change for the test
    from ghga_service_commons.utils.utc_dates import now_as_utc

    from srs.core.models import ExperimentalMetadata, Publication

    await metadata_dao.insert(
        ExperimentalMetadata(
            id=study.id, metadata={}, submitted=now_as_utc()
        )
    )
    await publication_dao.insert(
        Publication(
            **E["publications"]["pub1"],
            study_id=study.id,
            created=now_as_utc(),
        )
    )
    await controller.studies.update_study(
        study_id=study.id,
        status=StudyStatus.PERSISTED,
        approved_by=USER_STEWARD,
    )

    with pytest.raises(StudyRegistryPort.StatusConflictError):
        await controller.studies.delete_study(study_id=study.id)


@pytest.mark.asyncio
async def test_delete_study_not_found(controller):
    """Deleting a non-existent study must raise StudyNotFoundError."""
    with pytest.raises(StudyRegistryPort.StudyNotFoundError):
        await controller.studies.delete_study(study_id="NONEXIST")
