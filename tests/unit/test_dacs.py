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

"""Tests for DataAccessCommittee (DAC) user stories.

Spec: POST /dacs, GET /dacs, GET /dacs/{id}, PATCH /dacs/{id}, DELETE /dacs/{id}
"""

import pytest

from srs.ports.inbound.study_registry import StudyRegistryPort
from tests.fixtures.examples import EXAMPLES

E = EXAMPLES


# ── POST /dacs ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_dac(controller, dac_dao):
    """Creating a DAC must store it with active=True."""
    await controller.create_dac(**E["dacs"]["ethics"])
    dac = await dac_dao.get_by_id("DAC-001")
    assert dac.name == "Ethics Board"
    assert dac.active is True


@pytest.mark.asyncio
async def test_create_dac_duplicate(controller):
    """Creating a DAC with a duplicate ID must raise DuplicateError."""
    await controller.create_dac(**E["dacs"]["ethics"])
    with pytest.raises(StudyRegistryPort.DuplicateError):
        await controller.create_dac(
            id="DAC-001", name="Other", email="x@example.org", institute="Y",
        )


# ── GET /dacs ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_dacs_empty(controller):
    """Initially there must be no DACs."""
    dacs = await controller.get_dacs()
    assert dacs == []


@pytest.mark.asyncio
async def test_get_dacs_returns_all(controller):
    """Getting DACs must return all created DACs."""
    await controller.create_dac(**E["dacs"]["a"])
    await controller.create_dac(**E["dacs"]["b"])
    dacs = await controller.get_dacs()
    assert len(dacs) == 2


# ── GET /dacs/{id} ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_dac_by_id(controller):
    """Getting a DAC by ID must return the correct DAC."""
    await controller.create_dac(**E["dacs"]["default"])
    dac = await controller.get_dac(dac_id="DAC-1")
    assert dac.name == "Board"


@pytest.mark.asyncio
async def test_get_dac_not_found(controller):
    """Getting a non-existent DAC must raise DacNotFoundError."""
    with pytest.raises(StudyRegistryPort.DacNotFoundError):
        await controller.get_dac(dac_id="NONEXIST")


# ── PATCH /dacs/{id} ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_update_dac(controller):
    """Updating a DAC must persist the changes."""
    await controller.create_dac(**E["dacs"]["old"])
    await controller.update_dac(dac_id="DAC-1", name="New Name")
    dac = await controller.get_dac(dac_id="DAC-1")
    assert dac.name == "New Name"


@pytest.mark.asyncio
async def test_update_dac_deactivate(controller):
    """Deactivating a DAC must set active=False."""
    await controller.create_dac(**E["dacs"]["a"])
    await controller.update_dac(dac_id="DAC-1", active=False)
    dac = await controller.get_dac(dac_id="DAC-1")
    assert dac.active is False


@pytest.mark.asyncio
async def test_update_dac_not_found(controller):
    """Updating a non-existent DAC must raise DacNotFoundError."""
    with pytest.raises(StudyRegistryPort.DacNotFoundError):
        await controller.update_dac(dac_id="NONEXIST", name="X")


# ── DELETE /dacs/{id} ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_delete_dac(controller, dac_dao):
    """Deleting a DAC with no references must succeed."""
    await controller.create_dac(**E["dacs"]["a"])
    await controller.delete_dac(dac_id="DAC-1")
    assert "DAC-1" not in dac_dao.data


@pytest.mark.asyncio
async def test_delete_dac_not_found(controller):
    """Deleting a non-existent DAC must raise DacNotFoundError."""
    with pytest.raises(StudyRegistryPort.DacNotFoundError):
        await controller.delete_dac(dac_id="NONEXIST")


@pytest.mark.asyncio
async def test_delete_dac_with_referencing_dap(controller):
    """Deleting a DAC referenced by a DAP must raise ReferenceConflictError."""
    await controller.create_dac(**E["dacs"]["a"])
    await controller.create_dap(**E["daps"]["dacs_ref"])
    with pytest.raises(StudyRegistryPort.ReferenceConflictError):
        await controller.delete_dac(dac_id="DAC-1")
