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

"""Tests for DataAccessPolicy (DAP) user stories.

Spec: POST /daps, GET /daps, GET /daps/{id}, PATCH /daps/{id}, DELETE /daps/{id}
"""

import pytest

from srs.ports.inbound.study_registry import StudyRegistryPort
from tests.conftest import USER_SUBMITTER


# ── helpers ──────────────────────────────────────────────────────


async def _create_dac(controller, dac_id="DAC-1"):
    await controller.create_dac(
        id=dac_id,
        name="Board",
        email="board@example.org",
        institute="Inst",
    )


async def _create_dap(controller, dap_id="DAP-1", dac_id="DAC-1"):
    await controller.create_dap(
        id=dap_id,
        name="Policy",
        description="A policy",
        text="Policy text",
        url=None,
        duo_permission_id="DUO:0000042",  # GRU
        duo_modifier_ids=["DUO:0000011"],  # POA
        dac_id=dac_id,
    )


# ── POST /daps ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_dap(controller, dap_dao):
    """Creating a DAP must store it with active=True."""
    await _create_dac(controller)
    await _create_dap(controller)
    dap = await dap_dao.get_by_id("DAP-1")
    assert dap.name == "Policy"
    assert dap.active is True
    assert dap.dac_id == "DAC-1"


@pytest.mark.asyncio
async def test_create_dap_dac_not_found(controller):
    """Creating a DAP with a non-existent DAC must raise DacNotFoundError."""
    with pytest.raises(StudyRegistryPort.DacNotFoundError):
        await _create_dap(controller, dac_id="NONEXIST")


@pytest.mark.asyncio
async def test_create_dap_duplicate(controller):
    """Creating a DAP with a duplicate ID must raise DuplicateError."""
    await _create_dac(controller)
    await _create_dap(controller)
    with pytest.raises(StudyRegistryPort.DuplicateError):
        await _create_dap(controller, dap_id="DAP-1")


@pytest.mark.asyncio
async def test_create_dap_duo_enums(controller, dap_dao):
    """DAP DUO permission and modifier IDs must be stored as enums."""
    await _create_dac(controller)
    await _create_dap(controller)
    dap = await dap_dao.get_by_id("DAP-1")
    assert dap.duo_permission_id.value == "DUO:0000042"
    assert len(dap.duo_modifier_ids) == 1
    assert dap.duo_modifier_ids[0].value == "DUO:0000011"


# ── GET /daps ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_daps_empty(controller):
    """Initially there must be no DAPs."""
    daps = await controller.get_daps()
    assert daps == []


@pytest.mark.asyncio
async def test_get_daps_returns_all(controller):
    """Getting DAPs must return all created DAPs."""
    await _create_dac(controller)
    await _create_dap(controller, dap_id="DAP-1")
    await _create_dap(controller, dap_id="DAP-2")
    daps = await controller.get_daps()
    assert len(daps) == 2


# ── GET /daps/{id} ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_dap_by_id(controller):
    """Getting a DAP by ID must return the correct DAP."""
    await _create_dac(controller)
    await _create_dap(controller)
    dap = await controller.get_dap(dap_id="DAP-1")
    assert dap.name == "Policy"


@pytest.mark.asyncio
async def test_get_dap_not_found(controller):
    """Getting a non-existent DAP must raise DapNotFoundError."""
    with pytest.raises(StudyRegistryPort.DapNotFoundError):
        await controller.get_dap(dap_id="NONEXIST")


# ── PATCH /daps/{id} ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_update_dap_name(controller):
    """Updating a DAP name must persist the change."""
    await _create_dac(controller)
    await _create_dap(controller)
    await controller.update_dap(dap_id="DAP-1", name="New Name")
    dap = await controller.get_dap(dap_id="DAP-1")
    assert dap.name == "New Name"


@pytest.mark.asyncio
async def test_update_dap_change_dac(controller):
    """Changing the DAC for a DAP must verify the new DAC exists."""
    await _create_dac(controller, dac_id="DAC-1")
    await _create_dac(controller, dac_id="DAC-2")
    await _create_dap(controller)
    await controller.update_dap(dap_id="DAP-1", dac_id="DAC-2")
    dap = await controller.get_dap(dap_id="DAP-1")
    assert dap.dac_id == "DAC-2"


@pytest.mark.asyncio
async def test_update_dap_change_dac_not_found(controller):
    """Changing to a non-existent DAC must raise DacNotFoundError."""
    await _create_dac(controller)
    await _create_dap(controller)
    with pytest.raises(StudyRegistryPort.DacNotFoundError):
        await controller.update_dap(dap_id="DAP-1", dac_id="NONEXIST")


@pytest.mark.asyncio
async def test_update_dap_not_found(controller):
    """Updating a non-existent DAP must raise DapNotFoundError."""
    with pytest.raises(StudyRegistryPort.DapNotFoundError):
        await controller.update_dap(dap_id="NONEXIST", name="X")


# ── DELETE /daps/{id} ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_delete_dap(controller, dap_dao):
    """Deleting a DAP with no references must succeed."""
    await _create_dac(controller)
    await _create_dap(controller)
    await controller.delete_dap(dap_id="DAP-1")
    assert "DAP-1" not in dap_dao.data


@pytest.mark.asyncio
async def test_delete_dap_not_found(controller):
    """Deleting a non-existent DAP must raise DapNotFoundError."""
    with pytest.raises(StudyRegistryPort.DapNotFoundError):
        await controller.delete_dap(dap_id="NONEXIST")


@pytest.mark.asyncio
async def test_delete_dap_with_referencing_dataset(controller):
    """Deleting a DAP referenced by a dataset must raise ReferenceConflictError."""
    await _create_dac(controller)
    await _create_dap(controller)
    study = await controller.create_study(
        title="S",
        description="",
        types=[],
        affiliations=[],
        created_by=USER_SUBMITTER,
    )
    await controller.create_dataset(
        title="DS",
        description="desc",
        types=[],
        study_id=study.id,
        dap_id="DAP-1",
        files=[],
    )
    with pytest.raises(StudyRegistryPort.ReferenceConflictError):
        await controller.delete_dap(dap_id="DAP-1")
