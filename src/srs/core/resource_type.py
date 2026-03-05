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

"""Core implementation of ResourceType CRUD operations."""

import logging
from typing import Any
from uuid import UUID, uuid4

from ghga_service_commons.utils.utc_dates import UTCDatetime, now_as_utc
from hexkit.protocols.dao import ResourceNotFoundError

from srs.core.models import ResourceType, TypedResource
from srs.ports.inbound.resource_type import ResourceTypePort
from srs.ports.outbound.dao import DatasetDao, ResourceTypeDao, StudyDao

log = logging.getLogger(__name__)

class ResourceTypeController(ResourceTypePort):
    """Core implementation of ResourceType CRUD operations."""

    def __init__(
        self,
        *,
        resource_type_dao: ResourceTypeDao,
        study_dao: StudyDao,
        dataset_dao: DatasetDao,
    ):
        self._resource_type_dao = resource_type_dao
        self._study_dao = study_dao
        self._dataset_dao = dataset_dao

    # --- ResourceType operations ---

    async def create_resource_type(
        self,
        *,
        data: dict[str, Any],
    ) -> ResourceType:
        """Create a new resource type."""
        now = now_as_utc()
        data["code"] = data["code"].upper()
        rt = ResourceType(
            **data,
            id=uuid4(),
            created=now,
            changed=now,
            active=True,
        )
        await self._resource_type_dao.insert(rt)
        log.info("Created resource type %s (%s)", rt.code, rt.resource)
        return rt

    async def get_resource_types(
        self,
        *,
        resource: TypedResource | None = None,
        text: str | None = None,
        skip: int = 0,
        limit: int = 100,
    ) -> list[ResourceType]:
        """Get resource types filtered by optional parameters."""
        mapping: dict = {}
        if resource is not None:
            mapping["resource"] = resource.value

        types: list[ResourceType] = []
        async for rt in self._resource_type_dao.find_all(mapping=mapping):
            if text is not None:
                text_lower = text.lower()
                fields = [rt.name, rt.code]
                if rt.description:
                    fields.append(rt.description)
                if not any(text_lower in f.lower() for f in fields):
                    continue
            types.append(rt)

        return types[skip : skip + limit]

    async def get_resource_type(
        self, *, resource_type_id: UUID
    ) -> ResourceType:
        """Get a resource type by internal ID."""
        try:
            return await self._resource_type_dao.get_by_id(
                str(resource_type_id)
            )
        except ResourceNotFoundError as err:
            raise self.ResourceTypeNotFoundError(
                resource_type_id=resource_type_id
            ) from err

    async def update_resource_type(
        self,
        *,
        resource_type_id: UUID,
        updates: dict[str, str | Any] | None = None,
    ) -> None:
        """Update a resource type."""
        if not updates:
            return

        try:
            rt = await self._resource_type_dao.get_by_id(
                str(resource_type_id)
            )
        except ResourceNotFoundError as err:
            raise self.ResourceTypeNotFoundError(
                resource_type_id=resource_type_id
            ) from err

        updates["changed"] = now_as_utc()
        rt = rt.model_copy(update=updates)
        await self._resource_type_dao.update(rt)
        log.info("Updated resource type %s", resource_type_id)

    async def delete_resource_type(
        self, *, resource_type_id: UUID
    ) -> None:
        """Delete a resource type."""
        try:
            rt = await self._resource_type_dao.get_by_id(
                str(resource_type_id)
            )
        except ResourceNotFoundError as err:
            raise self.ResourceTypeNotFoundError(
                resource_type_id=resource_type_id
            ) from err

        # Check if still referenced by any study or dataset
        mapping_field = (
            "types"
            if rt.resource in (TypedResource.STUDY, TypedResource.DATASET)
            else None
        )
        if mapping_field:
            target_dao = (
                self._study_dao
                if rt.resource == TypedResource.STUDY
                else self._dataset_dao
            )
            async for entity in target_dao.find_all(mapping={}):
                if rt.code in entity.types:
                    raise self.ReferenceConflictError(
                        detail=f"Cannot delete resource type {rt.code}; "
                        f"it is still referenced by {entity.id}."
                    )

        await self._resource_type_dao.delete(str(resource_type_id))
        log.info("Deleted resource type %s", resource_type_id)
