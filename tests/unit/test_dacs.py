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

from srs.ports.inbound.data_access import DataAccessPort
from tests.fixtures.examples import EXAMPLES

E = EXAMPLES


# ── POST /dacs ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_dac(data_access, dac_dao):
    """Creating a DAC must store it with active=True."""
    await data_access.create_dac(data=E["dacs"]["ethics"])
    dac = await dac_dao.get_by_id("DAC-001")
    assert dac.name == "Ethics Board"
    assert dac.active is True


@pytest.mark.asyncio
async def test_create_dac_duplicate(data_access):
    """Creating a DAC with a duplicate ID must raise DuplicateError."""
    await data_access.create_dac(data=E["dacs"]["ethics"])
    with pytest.raises(DataAccessPort.DuplicateError):
        await data_access.create_dac(
            data={"id": "DAC-001", "name": "Other", "email": "x@example.org", "institute": "Y"},
        )


# ── GET /dacs ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_dacs_returns_all(data_access):
    """Getting DACs must return all created DACs."""
    await data_access.create_dac(data=E["dacs"]["a"])
    await data_access.create_dac(data=E["dacs"]["b"])
    dacs = await data_access.get_dacs()
    assert len(dacs) == 2


# ── GET /dacs/{id} ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_dac_by_id(data_access):
    """Getting a DAC by ID must return the correct DAC."""
    await data_access.create_dac(data=E["dacs"]["default"])
    dac = await data_access.get_dac(dac_id="DAC-1")
    assert dac.name == "Board"


@pytest.mark.asyncio
async def test_get_dac_not_found(data_access):
    """Getting a non-existent DAC must raise DacNotFoundError."""
    with pytest.raises(DataAccessPort.DacNotFoundError):
        await data_access.get_dac(dac_id="NONEXIST")


# ── PATCH /dacs/{id} ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_update_dac(data_access):
    """Updating a DAC must persist the changes."""
    await data_access.create_dac(data=E["dacs"]["default"])
    await data_access.update_dac(dac_id="DAC-1", updates={"name": "New Name"})
    dac = await data_access.get_dac(dac_id="DAC-1")
    assert dac.name == "New Name"


@pytest.mark.asyncio
async def test_update_dac_deactivate(data_access):
    """Deactivating a DAC must set active=False."""
    await data_access.create_dac(data=E["dacs"]["a"])
    await data_access.update_dac(dac_id="DAC-1", updates={"active": False})
    dac = await data_access.get_dac(dac_id="DAC-1")
    assert dac.active is False


@pytest.mark.asyncio
async def test_update_dac_not_found(data_access):
    """Updating a non-existent DAC must raise DacNotFoundError."""
    with pytest.raises(DataAccessPort.DacNotFoundError):
        await data_access.update_dac(dac_id="NONEXIST", updates={"name": "X"})


# ── DELETE /dacs/{id} ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_delete_dac(data_access, dac_dao):
    """Deleting a DAC with no references must succeed."""
    await data_access.create_dac(data=E["dacs"]["a"])
    await data_access.delete_dac(dac_id="DAC-1")
    assert "DAC-1" not in dac_dao.resources


@pytest.mark.asyncio
async def test_delete_dac_not_found(data_access):
    """Deleting a non-existent DAC must raise DacNotFoundError."""
    with pytest.raises(DataAccessPort.DacNotFoundError):
        await data_access.delete_dac(dac_id="NONEXIST")


@pytest.mark.asyncio
async def test_delete_dac_with_referencing_dap(data_access):
    """Deleting a DAC referenced by a DAP must raise ReferenceConflictError."""
    await data_access.create_dac(data=E["dacs"]["a"])
    await data_access.create_dap(data=E["daps"]["default"])
    with pytest.raises(DataAccessPort.ReferenceConflictError):
        await data_access.delete_dac(dac_id="DAC-1")
