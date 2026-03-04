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

"""Defines the Dataset inbound port for dataset operations."""

from abc import ABC, abstractmethod
from uuid import UUID

from srs.core.models import Dataset
from srs.ports.inbound.errors import (
    AccessDeniedError,
    DapNotFoundError,
    DatasetNotFoundError,
    RegistryError,
    StatusConflictError,
    StudyNotFoundError,
    ValidationError,
)


class DatasetPort(ABC):
    """Inbound port for Dataset operations."""

    # --- Error classes (shared via errors module) ---

    DatasetError = RegistryError
    DatasetNotFoundError = DatasetNotFoundError
    StudyNotFoundError = StudyNotFoundError
    DapNotFoundError = DapNotFoundError
    StatusConflictError = StatusConflictError
    ValidationError = ValidationError
    AccessDeniedError = AccessDeniedError

    # --- Dataset operations ---

    @abstractmethod
    async def create_dataset(
        self,
        *,
        title: str,
        description: str,
        types: list[str],
        study_id: str,
        dap_id: str,
        files: list[str],
    ) -> Dataset:
        """Create or update a dataset for a study.

        Raises:
        - StudyNotFoundError if the study does not exist.
        - StatusConflictError if the study is not in PENDING status.
        - DapNotFoundError if the DAP does not exist.
        - ValidationError if files don't exist in EM or are duplicated.
        """
        ...

    @abstractmethod
    async def get_datasets(
        self,
        *,
        dataset_type: str | None = None,
        study_id: str | None = None,
        text: str | None = None,
        skip: int = 0,
        limit: int = 100,
        user_id: UUID | None = None,
        is_data_steward: bool = False,
    ) -> list[Dataset]:
        """Get datasets filtered by optional parameters."""
        ...

    @abstractmethod
    async def get_dataset(
        self,
        *,
        dataset_id: str,
        user_id: UUID | None = None,
        is_data_steward: bool = False,
    ) -> Dataset:
        """Get a dataset by its PID.

        Raises:
        - DatasetNotFoundError if the dataset does not exist.
        - AccessDeniedError if the user does not have access.
        """
        ...

    @abstractmethod
    async def update_dataset(
        self, *, dataset_id: str, dap_id: str
    ) -> None:
        """Update the DAP assignment for a dataset.

        Raises:
        - DatasetNotFoundError if the dataset does not exist.
        - DapNotFoundError if the new DAP does not exist.
        """
        ...

    @abstractmethod
    async def delete_dataset(self, *, dataset_id: str) -> None:
        """Delete a dataset and its accession.

        Raises:
        - DatasetNotFoundError if the dataset does not exist.
        - StatusConflictError if the study is not in PENDING status.
        """
        ...
