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

"""Defines the Filename inbound port for filename operations."""

from abc import ABC, abstractmethod

from srs.ports.inbound.errors import (
    MetadataNotFoundError,
    RegistryError,
    StudyNotFoundError,
    ValidationError,
)


class FilenamePort(ABC):
    """Inbound port for Filename operations."""

    # --- Error classes (shared via errors module) ---

    FilenameError = RegistryError
    StudyNotFoundError = StudyNotFoundError
    MetadataNotFoundError = MetadataNotFoundError
    ValidationError = ValidationError

    # --- Filename operations ---

    @abstractmethod
    async def get_filenames(
        self, *, study_id: str
    ) -> dict[str, dict[str, str]]:
        """Get file accession to filename/alias mapping for a study.

        Returns a map from file accessions to {name, alias} objects.

        Raises:
        - StudyNotFoundError if the study does not exist.
        - MetadataNotFoundError if no accession map exists.
        """
        ...

    @abstractmethod
    async def post_filenames(
        self, *, study_id: str, file_id_map: dict[str, str]
    ) -> None:
        """Store file accession to internal file ID mappings.

        Creates AltAccession entries with type FILE_ID and republishes
        the mapping as an event.

        Raises:
        - StudyNotFoundError if the study does not exist.
        - ValidationError if accessions don't belong to the study.
        """
        ...
