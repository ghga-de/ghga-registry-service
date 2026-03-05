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

from srs.ports.inbound.data_access import DataAccessPort
from tests.conftest import USER_SUBMITTER
from tests.fixtures.examples import EXAMPLES

E = EXAMPLES


# ── helpers ──────────────────────────────────────────────────────


async def _create_dac(data_access, dac_id="DAC-1"):
    dac_data = {**E["dacs"]["default"], "id": dac_id}
    await data_access.create_dac(data=dac_data)


async def _create_dap(data_access, dap_id="DAP-1", dac_id="DAC-1"):
    dap_data = {**E["daps"]["with_modifiers"], "id": dap_id, "dac_id": dac_id}
    await data_access.create_dap(data=dap_data)


# ── POST /daps ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_dap(data_access, dap_dao):
    """Creating a DAP must store it with active=True."""
    await _create_dac(data_access)
    await _create_dap(data_access)
    dap = await dap_dao.get_by_id("DAP-1")
    assert dap.name == "Policy"
    assert dap.active is True
    assert dap.dac_id == "DAC-1"


@pytest.mark.asyncio
async def test_create_dap_dac_not_found(data_access):
    """Creating a DAP with a non-existent DAC must raise DacNotFoundError."""
    with pytest.raises(DataAccessPort.DacNotFoundError):
        await _create_dap(data_access, dac_id="NONEXIST")


@pytest.mark.asyncio
async def test_create_dap_duplicate(data_access):
    """Creating a DAP with a duplicate ID must raise DuplicateError."""
    await _create_dac(data_access)
    await _create_dap(data_access)
    with pytest.raises(DataAccessPort.DuplicateError):
        await _create_dap(data_access, dap_id="DAP-1")


@pytest.mark.asyncio
async def test_create_dap_duo_enums(data_access, dap_dao):
    """DAP DUO permission and modifier IDs must be stored as enums."""
    await _create_dac(data_access)
    await _create_dap(data_access)
    dap = await dap_dao.get_by_id("DAP-1")
    assert dap.duo_permission_id.value == "DUO:0000042"
    assert len(dap.duo_modifier_ids) == 1
    assert dap.duo_modifier_ids[0].value == "DUO:0000011"


# ── GET /daps ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_daps_empty(data_access):
    """Initially there must be no DAPs."""
    daps = await data_access.get_daps()
    assert daps == []


@pytest.mark.asyncio
async def test_get_daps_returns_all(data_access):
    """Getting DAPs must return all created DAPs."""
    await _create_dac(data_access)
    await _create_dap(data_access, dap_id="DAP-1")
    await _create_dap(data_access, dap_id="DAP-2")
    daps = await data_access.get_daps()
    assert len(daps) == 2


# ── GET /daps/{id} ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_dap_by_id(data_access):
    """Getting a DAP by ID must return the correct DAP."""
    await _create_dac(data_access)
    await _create_dap(data_access)
    dap = await data_access.get_dap(dap_id="DAP-1")
    assert dap.name == "Policy"


@pytest.mark.asyncio
async def test_get_dap_not_found(data_access):
    """Getting a non-existent DAP must raise DapNotFoundError."""
    with pytest.raises(DataAccessPort.DapNotFoundError):
        await data_access.get_dap(dap_id="NONEXIST")


# ── PATCH /daps/{id} ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_update_dap_name(data_access):
    """Updating a DAP name must persist the change."""
    await _create_dac(data_access)
    await _create_dap(data_access)
    await data_access.update_dap(dap_id="DAP-1", updates={"name": "New Name"})
    dap = await data_access.get_dap(dap_id="DAP-1")
    assert dap.name == "New Name"


@pytest.mark.asyncio
async def test_update_dap_change_dac(data_access):
    """Changing the DAC for a DAP must verify the new DAC exists."""
    await _create_dac(data_access, dac_id="DAC-1")
    await _create_dac(data_access, dac_id="DAC-2")
    await _create_dap(data_access)
    await data_access.update_dap(dap_id="DAP-1", updates={"dac_id": "DAC-2"})
    dap = await data_access.get_dap(dap_id="DAP-1")
    assert dap.dac_id == "DAC-2"


@pytest.mark.asyncio
async def test_update_dap_change_dac_not_found(data_access):
    """Changing to a non-existent DAC must raise DacNotFoundError."""
    await _create_dac(data_access)
    await _create_dap(data_access)
    with pytest.raises(DataAccessPort.DacNotFoundError):
        await data_access.update_dap(dap_id="DAP-1", updates={"dac_id": "NONEXIST"})


@pytest.mark.asyncio
async def test_update_dap_not_found(data_access):
    """Updating a non-existent DAP must raise DapNotFoundError."""
    with pytest.raises(DataAccessPort.DapNotFoundError):
        await data_access.update_dap(dap_id="NONEXIST", updates={"name": "X"})


# ── DELETE /daps/{id} ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_delete_dap(data_access, dap_dao):
    """Deleting a DAP with no references must succeed."""
    await _create_dac(data_access)
    await _create_dap(data_access)
    await data_access.delete_dap(dap_id="DAP-1")
    assert "DAP-1" not in dap_dao.resources


@pytest.mark.asyncio
async def test_delete_dap_not_found(data_access):
    """Deleting a non-existent DAP must raise DapNotFoundError."""
    with pytest.raises(DataAccessPort.DapNotFoundError):
        await data_access.delete_dap(dap_id="NONEXIST")


@pytest.mark.asyncio
async def test_delete_dap_with_referencing_dataset(data_access, controller):
    """Deleting a DAP referenced by a dataset must raise ReferenceConflictError."""
    await _create_dac(data_access)
    await _create_dap(data_access)
    study = await controller.studies.create_study(
        data={**E["studies"]["minimal"], "created_by": USER_SUBMITTER},
    )
    await controller.datasets.create_dataset(
        data={**E["datasets"]["minimal"], "study_id": study.id, "dap_id": "DAP-1"},
    )
    with pytest.raises(DataAccessPort.ReferenceConflictError):
        await data_access.delete_dap(dap_id="DAP-1")
