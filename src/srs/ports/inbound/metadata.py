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

"""Defines the Metadata inbound port for experimental metadata operations."""

from abc import ABC, abstractmethod

from srs.core.models import ExperimentalMetadata
from srs.ports.inbound.errors import (
    MetadataNotFoundError,
    RegistryError,
    StatusConflictError,
    StudyNotFoundError,
)


class MetadataPort(ABC):
    """Inbound port for Experimental Metadata operations."""

    # --- Error classes (shared via errors module) ---

    MetadataError = RegistryError
    StudyNotFoundError = StudyNotFoundError
    StatusConflictError = StatusConflictError
    MetadataNotFoundError = MetadataNotFoundError

    # --- Metadata operations ---

    @abstractmethod
    async def upsert_metadata(
        self, *, study_id: str, metadata: dict
    ) -> None:
        """Create or update experimental metadata for a study.

        Raises:
        - StudyNotFoundError if the study does not exist.
        - StatusConflictError if the study is not in PENDING status.
        """
        ...

    @abstractmethod
    async def get_metadata(self, *, study_id: str) -> ExperimentalMetadata:
        """Get experimental metadata for a study.

        Raises:
        - MetadataNotFoundError if metadata is not found.
        """
        ...

    @abstractmethod
    async def delete_metadata(self, *, study_id: str) -> None:
        """Delete experimental metadata for a study.

        Raises:
        - StudyNotFoundError if the study does not exist.
        - StatusConflictError if the study is not in PENDING status.
        - MetadataNotFoundError if metadata is not found.
        """
        ...
