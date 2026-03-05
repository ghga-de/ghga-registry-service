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

"""Defines the Data Access inbound port for DAC and DAP operations."""

from abc import ABC, abstractmethod
from typing import Any

from srs.core.models import DataAccessCommittee, DataAccessPolicy
from srs.ports.inbound.errors import (
    DacNotFoundError,
    DapNotFoundError,
    DuplicateError,
    ReferenceConflictError,
    RegistryError,
)


class DataAccessPort(ABC):
    """Inbound port for Data Access Committee and Data Access Policy operations."""

    # --- Error classes (shared via errors module) ---

    DataAccessError = RegistryError
    DacNotFoundError = DacNotFoundError
    DapNotFoundError = DapNotFoundError
    DuplicateError = DuplicateError
    ReferenceConflictError = ReferenceConflictError

    # --- DAC operations ---

    @abstractmethod
    async def create_dac(
        self,
        *,
        data: dict[str, Any],
    ) -> None:
        """Create a new DAC.

        Raises:
        - DuplicateError if a DAC with this ID already exists.
        """
        ...

    @abstractmethod
    async def get_dacs(self) -> list[DataAccessCommittee]:
        """Get all DACs."""
        ...

    @abstractmethod
    async def get_dac(self, *, dac_id: str) -> DataAccessCommittee:
        """Get a DAC by ID.

        Raises:
        - DacNotFoundError if the DAC does not exist.
        """
        ...

    @abstractmethod
    async def update_dac(
        self,
        *,
        dac_id: str,
        updates: dict[str, str | Any] | None = None,
    ) -> None:
        """Update a DAC.

        Raises:
        - DacNotFoundError if the DAC does not exist.
        """
        ...

    @abstractmethod
    async def delete_dac(self, *, dac_id: str) -> None:
        """Delete a DAC.

        Raises:
        - DacNotFoundError if the DAC does not exist.
        - ReferenceConflictError if the DAC is referenced by any DAP.
        """
        ...

    # --- DAP operations ---

    @abstractmethod
    async def create_dap(
        self,
        *,
        data: dict[str, Any],
    ) -> None:
        """Create a new DAP.

        Raises:
        - DacNotFoundError if the referenced DAC does not exist.
        - DuplicateError if a DAP with this ID already exists.
        """
        ...

    @abstractmethod
    async def get_daps(self) -> list[DataAccessPolicy]:
        """Get all DAPs."""
        ...

    @abstractmethod
    async def get_dap(self, *, dap_id: str) -> DataAccessPolicy:
        """Get a DAP by ID.

        Raises:
        - DapNotFoundError if the DAP does not exist.
        """
        ...

    @abstractmethod
    async def update_dap(
        self,
        *,
        dap_id: str,
        updates: dict[str, str | Any] | None = None,
    ) -> None:
        """Update a DAP.

        Raises:
        - DapNotFoundError if the DAP does not exist.
        - DacNotFoundError if the new DAC does not exist.
        """
        ...

    @abstractmethod
    async def delete_dap(self, *, dap_id: str) -> None:
        """Delete a DAP.

        Raises:
        - DapNotFoundError if the DAP does not exist.
        - ReferenceConflictError if the DAP is referenced by any dataset.
        """
        ...
