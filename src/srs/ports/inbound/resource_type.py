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

"""Defines the ResourceType inbound port."""

from abc import ABC, abstractmethod
from typing import Any
from uuid import UUID

from srs.core.models import ResourceType, TypedResource
from srs.ports.inbound.errors import (
    ReferenceConflictError,
    RegistryError,
    ResourceTypeNotFoundError,
)


class ResourceTypePort(ABC):
    """Inbound port for ResourceType CRUD operations."""

    # --- Error classes (shared via errors module) ---

    ResourceTypeError = RegistryError
    ResourceTypeNotFoundError = ResourceTypeNotFoundError
    ReferenceConflictError = ReferenceConflictError

    # --- ResourceType operations ---

    @abstractmethod
    async def create_resource_type(
        self,
        *,
        data: dict[str, Any],
    ) -> ResourceType:
        """Create a new resource type.

        Returns the created ResourceType with active=True.
        """
        ...

    @abstractmethod
    async def get_resource_types(
        self,
        *,
        resource: TypedResource | None = None,
        text: str | None = None,
        skip: int = 0,
        limit: int = 100,
    ) -> list[ResourceType]:
        """Get resource types filtered by optional parameters."""
        ...

    @abstractmethod
    async def get_resource_type(
        self, *, resource_type_id: UUID
    ) -> ResourceType:
        """Get a resource type by internal ID.

        Raises:
        - ResourceTypeNotFoundError if the resource type does not exist.
        """
        ...

    @abstractmethod
    async def update_resource_type(
        self,
        *,
        resource_type_id: UUID,
        updates: dict[str, str | Any] | None = None,
    ) -> None:
        """Update a resource type.

        Raises:
        - ResourceTypeNotFoundError if the resource type does not exist.
        """
        ...

    @abstractmethod
    async def delete_resource_type(
        self, *, resource_type_id: UUID
    ) -> None:
        """Delete a resource type.

        Raises:
        - ResourceTypeNotFoundError if the resource type does not exist.
        - ReferenceConflictError if the type is still referenced.
        """
        ...
