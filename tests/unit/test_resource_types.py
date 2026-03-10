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

"""Tests for ResourceType user stories.

Spec: POST /resource-types, GET /resource-types, GET /resource-types/{id},
PATCH /resource-types/{id}, DELETE /resource-types/{id}
"""

import pytest

from srs.core.models import TypedResource
from srs.ports.inbound.study_registry import StudyRegistryPort
from tests.conftest import USER_SUBMITTER
from tests.fixtures.examples import EXAMPLES

E = EXAMPLES


# ── POST /resource-types ────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_resource_type(controller, resource_type_dao):
    """Creating a resource type must persist it with active=True."""
    rt = await controller.resource_types.create_resource_type(data=E["resource_types"]["wgs_study"])
    assert rt.code == "WGS"  # code is uppercased
    assert rt.active is True
    stored = await resource_type_dao.get_by_id(str(rt.id))
    assert stored.name == "Whole Genome Sequencing"


@pytest.mark.asyncio
async def test_create_resource_type_uppercases_code(controller):
    """The resource type code must be uppercased on creation."""
    rt = await controller.resource_types.create_resource_type(data=E["resource_types"]["rna_seq_dataset"])
    assert rt.code == "RNA-SEQ"


# ── GET /resource-types ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_resource_types_filter_by_resource(controller):
    """Filtering by resource must only return matching resource types."""
    await controller.resource_types.create_resource_type(data=E["resource_types"]["wgs_minimal"])
    await controller.resource_types.create_resource_type(data=E["resource_types"]["bam_dataset"])
    result = await controller.resource_types.get_resource_types(
        resource=TypedResource.STUDY
    )
    assert len(result) == 1
    assert result[0].code == "WGS"


@pytest.mark.asyncio
async def test_get_resource_types_text_filter(controller):
    """Text filter must match partial text in name, code, or description."""
    await controller.resource_types.create_resource_type(data=E["resource_types"]["wgs_full_desc"])
    await controller.resource_types.create_resource_type(data=E["resource_types"]["rna_study"])
    result = await controller.resource_types.get_resource_types(text="genome")
    assert len(result) == 1
    assert result[0].code == "WGS"


@pytest.mark.asyncio
async def test_get_resource_types_pagination(controller):
    """Skip and limit must paginate results."""
    for i in range(5):
        await controller.resource_types.create_resource_type(
            data={"code": f"T{i}", "resource": TypedResource.STUDY, "name": f"Type {i}", "description": None},
        )
    result = await controller.resource_types.get_resource_types(skip=2, limit=2)
    assert len(result) == 2


# ── GET /resource-types/{id} ────────────────────────────────────


@pytest.mark.asyncio
async def test_get_resource_type_by_id(controller):
    """Getting a resource type by ID must return it."""
    rt = await controller.resource_types.create_resource_type(data=E["resource_types"]["wgs_minimal"])
    fetched = await controller.resource_types.get_resource_type(resource_type_id=rt.id)
    assert fetched.code == "WGS"


@pytest.mark.asyncio
async def test_get_resource_type_not_found(controller):
    """Getting a non-existent resource type must raise ResourceTypeNotFoundError."""
    from uuid import uuid4

    with pytest.raises(StudyRegistryPort.ResourceTypeNotFoundError):
        await controller.resource_types.get_resource_type(resource_type_id=uuid4())


# ── PATCH /resource-types/{id} ──────────────────────────────────


@pytest.mark.asyncio
async def test_update_resource_type(controller):
    """Updating a resource type must persist the changes."""
    rt = await controller.resource_types.create_resource_type(data=E["resource_types"]["wgs_minimal"])
    await controller.resource_types.update_resource_type(
        resource_type_id=rt.id, updates={"name": "Whole Genome Seq"}
    )
    updated = await controller.resource_types.get_resource_type(resource_type_id=rt.id)
    assert updated.name == "Whole Genome Seq"


@pytest.mark.asyncio
async def test_update_resource_type_deactivate(controller):
    """Deactivating a resource type must set active=False."""
    rt = await controller.resource_types.create_resource_type(data=E["resource_types"]["wgs_minimal"])
    await controller.resource_types.update_resource_type(
        resource_type_id=rt.id, updates={"active": False}
    )
    updated = await controller.resource_types.get_resource_type(resource_type_id=rt.id)
    assert updated.active is False


@pytest.mark.asyncio
async def test_update_resource_type_not_found(controller):
    """Updating a non-existent resource type must raise ResourceTypeNotFoundError."""
    from uuid import uuid4

    with pytest.raises(StudyRegistryPort.ResourceTypeNotFoundError):
        await controller.resource_types.update_resource_type(
            resource_type_id=uuid4(), updates={"name": "X"}
        )


# ── DELETE /resource-types/{id} ─────────────────────────────────


@pytest.mark.asyncio
async def test_delete_resource_type(controller, resource_type_dao):
    """Deleting an unreferenced resource type must succeed."""
    rt = await controller.resource_types.create_resource_type(data=E["resource_types"]["wgs_minimal"])
    await controller.resource_types.delete_resource_type(resource_type_id=rt.id)
    assert rt.id not in resource_type_dao.resources


@pytest.mark.asyncio
async def test_delete_resource_type_not_found(controller):
    """Deleting a non-existent resource type must raise ResourceTypeNotFoundError."""
    from uuid import uuid4

    with pytest.raises(StudyRegistryPort.ResourceTypeNotFoundError):
        await controller.resource_types.delete_resource_type(resource_type_id=uuid4())


@pytest.mark.asyncio
async def test_delete_resource_type_still_referenced(controller):
    """Deleting a resource type still used by a study must raise ReferenceConflictError."""
    rt = await controller.resource_types.create_resource_type(data=E["resource_types"]["wgs_minimal"])
    await controller.studies.create_study(
        data={**E["studies"]["minimal"], "types": ["WGS"], "created_by": USER_SUBMITTER},
    )
    with pytest.raises(StudyRegistryPort.ReferenceConflictError):
        await controller.resource_types.delete_resource_type(resource_type_id=rt.id)
