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

"""FastAPI routes for Resource Type operations."""

import logging
from uuid import UUID

from fastapi import APIRouter, Query, status

from srs.adapters.inbound.fastapi_ import dummies
from srs.adapters.inbound.fastapi_.http_authorization import StewardAuthContext
from srs.adapters.inbound.fastapi_.http_exceptions import (
    HttpInternalError,
    HttpReferenceConflictError,
    HttpResourceTypeNotFoundError,
)
from srs.adapters.inbound.fastapi_.rest_models import (
    ResourceTypeCreateRequest,
    ResourceTypeUpdateRequest,
)
from srs.core.models import ResourceType, TypedResource
from srs.ports.inbound.resource_type import ResourceTypePort

log = logging.getLogger(__name__)

resource_type_router = APIRouter(tags=["Resource Types"])

# ──────────────────────── Resource Types ────────────────────────


@resource_type_router.post(
    "/resource-types",
    summary="Create a resource type",
    operation_id="createResourceType",
    status_code=status.HTTP_201_CREATED,
    response_model=ResourceType,
)
async def create_resource_type(
    body: ResourceTypeCreateRequest,
    auth: StewardAuthContext,
    registry: dummies.StudyRegistryDummy,
):
    """Create a new resource type."""
    try:
        return await registry.resource_types.create_resource_type(
            data=body.model_dump(),
        )
    except Exception as err:
        log.exception("Unexpected error in create_resource_type")
        raise HttpInternalError() from err


@resource_type_router.get(
    "/resource-types",
    summary="List resource types",
    operation_id="getResourceTypes",
    response_model=list[ResourceType],
)
async def get_resource_types(
    registry: dummies.StudyRegistryDummy,
    resource: str | None = Query(None),
    text: str | None = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """Get a filtered list of resource types."""
    try:
        typed_resource = TypedResource(resource) if resource else None
        return await registry.resource_types.get_resource_types(
            resource=typed_resource, text=text, skip=skip, limit=limit
        )
    except Exception as err:
        log.exception("Unexpected error in get_resource_types")
        raise HttpInternalError() from err


@resource_type_router.get(
    "/resource-types/{resource_type_id}",
    summary="Get a resource type",
    operation_id="getResourceType",
    response_model=ResourceType,
)
async def get_resource_type(
    resource_type_id: UUID,
    registry: dummies.StudyRegistryDummy,
):
    """Get a resource type by ID."""
    try:
        return await registry.resource_types.get_resource_type(
            resource_type_id=resource_type_id
        )
    except ResourceTypePort.ResourceTypeNotFoundError as err:
        raise HttpResourceTypeNotFoundError() from err
    except Exception as err:
        log.exception("Unexpected error in get_resource_type")
        raise HttpInternalError() from err


@resource_type_router.patch(
    "/resource-types/{resource_type_id}",
    summary="Update a resource type",
    operation_id="updateResourceType",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def update_resource_type(
    resource_type_id: UUID,
    body: ResourceTypeUpdateRequest,
    auth: StewardAuthContext,
    registry: dummies.StudyRegistryDummy,
):
    """Update a resource type."""
    try:
        updates = body.model_dump(exclude_unset=True, exclude_none=True)
        await registry.resource_types.update_resource_type(
            resource_type_id=resource_type_id,
            updates=updates,
        )
    except ResourceTypePort.ResourceTypeNotFoundError as err:
        raise HttpResourceTypeNotFoundError() from err
    except Exception as err:
        log.exception("Unexpected error in update_resource_type")
        raise HttpInternalError() from err


@resource_type_router.delete(
    "/resource-types/{resource_type_id}",
    summary="Delete a resource type",
    operation_id="deleteResourceType",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_resource_type(
    resource_type_id: UUID,
    auth: StewardAuthContext,
    registry: dummies.StudyRegistryDummy,
):
    """Delete a resource type."""
    try:
        await registry.resource_types.delete_resource_type(
            resource_type_id=resource_type_id
        )
    except ResourceTypePort.ResourceTypeNotFoundError as err:
        raise HttpResourceTypeNotFoundError() from err
    except ResourceTypePort.ReferenceConflictError as err:
        raise HttpReferenceConflictError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in delete_resource_type")
        raise HttpInternalError() from err
