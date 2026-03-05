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

"""Shared helper functions for core controllers."""

from uuid import UUID

from hexkit.protocols.dao import ResourceNotFoundError

from srs.core.models import Study, StudyStatus
from srs.ports.inbound.errors import (
    AccessDeniedError,
    StatusConflictError,
    StudyNotFoundError,
)
from srs.ports.outbound.dao import StudyDao


async def get_study_or_raise(study_dao: StudyDao, study_id: str) -> Study:
    """Retrieve a study by ID or raise StudyNotFoundError."""
    try:
        return await study_dao.get_by_id(study_id)
    except ResourceNotFoundError as err:
        raise StudyNotFoundError(study_id=study_id) from err


async def require_pending(study: Study) -> None:
    """Raise StatusConflictError if the study is not PENDING."""
    if study.status != StudyStatus.PENDING:
        raise StatusConflictError(
            detail=f"Study {study.id} has status {study.status}; "
            "expected PENDING."
        )


def check_user_access(
    study: Study,
    user_id: UUID | None,
    is_data_steward: bool,
) -> None:
    """Check if a user has access to a study."""
    if is_data_steward:
        return
    if study.users is None:
        return  # publicly accessible
    if user_id is not None and user_id in study.users:
        return
    raise AccessDeniedError()
