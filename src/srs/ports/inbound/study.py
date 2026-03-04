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

"""Defines the Study inbound port for study CRUD and publish operations."""

from abc import ABC, abstractmethod
from uuid import UUID

from srs.core.models import Study, StudyStatus
from srs.ports.inbound.errors import (
    AccessDeniedError,
    RegistryError,
    StatusConflictError,
    StudyNotFoundError,
    ValidationError,
)


class StudyPort(ABC):
    """Inbound port for Study CRUD and Publish operations."""

    # --- Error classes (shared via errors module) ---

    StudyError = RegistryError
    StudyNotFoundError = StudyNotFoundError
    StatusConflictError = StatusConflictError
    ValidationError = ValidationError
    AccessDeniedError = AccessDeniedError

    # --- Study operations ---

    @abstractmethod
    async def create_study(
        self,
        *,
        title: str,
        description: str,
        types: list[str],
        affiliations: list[str],
        created_by: UUID,
    ) -> Study:
        """Create a new study with status PENDING.

        Returns the created Study with generated PID.
        """
        ...

    @abstractmethod
    async def get_studies(
        self,
        *,
        status: StudyStatus | None = None,
        study_type: str | None = None,
        text: str | None = None,
        skip: int = 0,
        limit: int = 100,
        user_id: UUID | None = None,
        is_data_steward: bool = False,
    ) -> list[Study]:
        """Get studies filtered by optional parameters.

        Only returns studies accessible to the user.
        """
        ...

    @abstractmethod
    async def get_study(
        self,
        *,
        study_id: str,
        user_id: UUID | None = None,
        is_data_steward: bool = False,
    ) -> Study:
        """Get a study by its PID.

        Raises:
        - StudyNotFoundError if the study does not exist.
        - AccessDeniedError if the user does not have access.
        """
        ...

    @abstractmethod
    async def update_study(
        self,
        *,
        study_id: str,
        status: StudyStatus | None = None,
        users: list[UUID] | None = None,
        approved_by: UUID | None = None,
    ) -> None:
        """Update study status and/or users.

        Raises:
        - StudyNotFoundError if the study does not exist.
        - StatusConflictError if the status transition is not allowed.
        - ValidationError if validation fails during status change.
        """
        ...

    @abstractmethod
    async def delete_study(self, *, study_id: str) -> None:
        """Delete a study and all related entities.

        Raises:
        - StudyNotFoundError if the study does not exist.
        - StatusConflictError if the study is not in PENDING status.
        """
        ...

    # --- Publish operations ---

    @abstractmethod
    async def publish_study(self, *, study_id: str) -> None:
        """Validate and publish a study.

        Creates accession numbers for EM resources and publishes
        an AnnotatedExperimentalMetadata event.

        Raises:
        - StudyNotFoundError if the study does not exist.
        - ValidationError if the study is not complete or valid.
        """
        ...
