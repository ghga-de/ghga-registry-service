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

"""Defines the Publication inbound port for publication operations."""

from abc import ABC, abstractmethod
from uuid import UUID

from srs.core.models import Publication
from srs.ports.inbound.errors import (
    AccessDeniedError,
    PublicationNotFoundError,
    RegistryError,
    StatusConflictError,
    StudyNotFoundError,
)


class PublicationPort(ABC):
    """Inbound port for Publication operations."""

    # --- Error classes (shared via errors module) ---

    PublicationError = RegistryError
    StudyNotFoundError = StudyNotFoundError
    StatusConflictError = StatusConflictError
    PublicationNotFoundError = PublicationNotFoundError
    AccessDeniedError = AccessDeniedError

    # --- Publication operations ---

    @abstractmethod
    async def create_publication(
        self,
        *,
        title: str,
        abstract: str | None,
        authors: list[str],
        year: int,
        journal: str | None,
        doi: str | None,
        study_id: str,
    ) -> Publication:
        """Create a publication for a study.

        Raises:
        - StudyNotFoundError if the study does not exist.
        - StatusConflictError if the study is not in PENDING status.
        """
        ...

    @abstractmethod
    async def get_publications(
        self,
        *,
        year: int | None = None,
        text: str | None = None,
        skip: int = 0,
        limit: int = 100,
        user_id: UUID | None = None,
        is_data_steward: bool = False,
    ) -> list[Publication]:
        """Get publications filtered by optional parameters."""
        ...

    @abstractmethod
    async def get_publication(
        self,
        *,
        publication_id: str,
        user_id: UUID | None = None,
        is_data_steward: bool = False,
    ) -> Publication:
        """Get a publication by its PID.

        Raises:
        - PublicationNotFoundError if the publication does not exist.
        - AccessDeniedError if the user does not have access.
        """
        ...

    @abstractmethod
    async def delete_publication(self, *, publication_id: str) -> None:
        """Delete a publication and its accession.

        Raises:
        - PublicationNotFoundError if the publication does not exist.
        - StatusConflictError if the study is not in PENDING status.
        """
        ...
