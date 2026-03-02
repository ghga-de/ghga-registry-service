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

"""Tests for Authorization user stories.

Spec: Non-public studies require auth; stewards bypass access checks;
user fields are stripped for non-stewards; users list controls access.
"""

import pytest

from srs.core.models import StudyStatus
from srs.ports.inbound.study_registry import StudyRegistryPort
from tests.conftest import USER_OTHER, USER_STEWARD, USER_SUBMITTER


# ── Study-level access control ──────────────────────────────────


@pytest.mark.asyncio
async def test_steward_bypasses_access_control(controller):
    """Data stewards must be able to access any study regardless of users list."""
    study = await controller.create_study(
        title="Private",
        description="",
        types=[],
        affiliations=[],
        created_by=USER_SUBMITTER,
    )
    # No error for steward
    fetched = await controller.get_study(
        study_id=study.id,
        user_id=USER_STEWARD,
        is_data_steward=True,
    )
    assert fetched.title == "Private"


@pytest.mark.asyncio
async def test_listed_user_has_access(controller):
    """A user in the study's users list must have access."""
    study = await controller.create_study(
        title="Study",
        description="",
        types=[],
        affiliations=[],
        created_by=USER_SUBMITTER,
    )
    fetched = await controller.get_study(
        study_id=study.id, user_id=USER_SUBMITTER
    )
    assert fetched.title == "Study"


@pytest.mark.asyncio
async def test_unlisted_user_denied(controller):
    """A user NOT in the study's users list must be denied."""
    study = await controller.create_study(
        title="Study",
        description="",
        types=[],
        affiliations=[],
        created_by=USER_SUBMITTER,
    )
    with pytest.raises(StudyRegistryPort.AccessDeniedError):
        await controller.get_study(
            study_id=study.id, user_id=USER_OTHER
        )


@pytest.mark.asyncio
async def test_no_user_id_denied_for_private_study(controller):
    """Calling without a user_id for a private study must be denied."""
    study = await controller.create_study(
        title="Study",
        description="",
        types=[],
        affiliations=[],
        created_by=USER_SUBMITTER,
    )
    with pytest.raises(StudyRegistryPort.AccessDeniedError):
        await controller.get_study(study_id=study.id, user_id=None)


@pytest.mark.asyncio
async def test_public_study_accessible_without_auth(controller, study_dao):
    """A study with users=None (public) must be accessible to anyone."""
    study = await controller.create_study(
        title="Public Study",
        description="",
        types=[],
        affiliations=[],
        created_by=USER_SUBMITTER,
    )
    # Make it public by setting users=None directly in the DAO
    updated = study.model_copy(update={"users": None})
    await study_dao.update(updated)

    fetched = await controller.get_study(
        study_id=study.id, user_id=None
    )
    assert fetched.title == "Public Study"


# ── User field stripping ────────────────────────────────────────


@pytest.mark.asyncio
async def test_steward_sees_user_fields(controller):
    """Data stewards must see users, created_by, and approved_by."""
    study = await controller.create_study(
        title="Study",
        description="",
        types=[],
        affiliations=[],
        created_by=USER_SUBMITTER,
    )
    fetched = await controller.get_study(
        study_id=study.id,
        user_id=USER_STEWARD,
        is_data_steward=True,
    )
    assert fetched.users == [USER_SUBMITTER]
    assert fetched.created_by == USER_SUBMITTER


@pytest.mark.asyncio
async def test_non_steward_user_fields_stripped(controller):
    """Non-steward users must NOT see user-related fields."""
    study = await controller.create_study(
        title="Study",
        description="",
        types=[],
        affiliations=[],
        created_by=USER_SUBMITTER,
    )
    fetched = await controller.get_study(
        study_id=study.id, user_id=USER_SUBMITTER
    )
    assert fetched.users is None
    assert fetched.approved_by is None


@pytest.mark.asyncio
async def test_get_studies_strips_for_non_steward(controller):
    """List endpoint must also strip user fields for non-stewards."""
    await controller.create_study(
        title="Study",
        description="",
        types=[],
        affiliations=[],
        created_by=USER_SUBMITTER,
    )
    result = await controller.get_studies(user_id=USER_SUBMITTER)
    assert len(result) == 1
    assert result[0].users is None


@pytest.mark.asyncio
async def test_get_studies_steward_keeps_user_fields(controller):
    """List endpoint must keep user fields for stewards."""
    await controller.create_study(
        title="Study",
        description="",
        types=[],
        affiliations=[],
        created_by=USER_SUBMITTER,
    )
    result = await controller.get_studies(
        user_id=USER_STEWARD, is_data_steward=True
    )
    assert len(result) == 1
    assert result[0].users == [USER_SUBMITTER]


# ── Update users list ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_update_study_users_list(controller, study_dao):
    """Stewards can update the users list to add or remove users."""
    study = await controller.create_study(
        title="Study",
        description="",
        types=[],
        affiliations=[],
        created_by=USER_SUBMITTER,
    )
    await controller.update_study(
        study_id=study.id,
        users=[USER_SUBMITTER, USER_OTHER],
    )
    updated = await study_dao.get_by_id(study.id)
    assert USER_OTHER in updated.users
    assert USER_SUBMITTER in updated.users


# ── Cross-entity access control ─────────────────────────────────


@pytest.mark.asyncio
async def test_publication_access_denied_for_unauthorized_user(controller):
    """Publication access must be checked via the parent study's users list."""
    study = await controller.create_study(
        title="Study",
        description="",
        types=[],
        affiliations=[],
        created_by=USER_SUBMITTER,
    )
    pub = await controller.create_publication(
        title="P",
        abstract=None,
        authors=["A"],
        year=2025,
        journal=None,
        doi=None,
        study_id=study.id,
    )
    with pytest.raises(StudyRegistryPort.AccessDeniedError):
        await controller.get_publication(
            publication_id=pub.id, user_id=USER_OTHER
        )


@pytest.mark.asyncio
async def test_dataset_access_denied_for_unauthorized_user(controller):
    """Dataset access must be checked via the parent study's users list."""
    study = await controller.create_study(
        title="Study",
        description="",
        types=[],
        affiliations=[],
        created_by=USER_SUBMITTER,
    )
    await controller.create_dac(
        id="DAC-1",
        name="B",
        email="b@example.org",
        institute="I",
    )
    await controller.create_dap(
        id="DAP-1",
        name="P",
        description="d",
        text="t",
        url=None,
        duo_permission_id="DUO:0000042",
        duo_modifier_ids=[],
        dac_id="DAC-1",
    )
    ds = await controller.create_dataset(
        title="DS",
        description="d",
        types=[],
        study_id=study.id,
        dap_id="DAP-1",
        files=[],
    )
    with pytest.raises(StudyRegistryPort.AccessDeniedError):
        await controller.get_dataset(
            dataset_id=ds.id, user_id=USER_OTHER
        )
