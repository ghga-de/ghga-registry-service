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

"""Core implementation of Data Access Committee and Data Access Policy operations."""

import logging

from ghga_service_commons.utils.utc_dates import now_as_utc
from hexkit.protocols.dao import ResourceNotFoundError

from srs.core.models import (
    DataAccessCommittee,
    DataAccessPolicy,
    DuoModifier,
    DuoPermission,
)
from srs.ports.inbound.data_access import DataAccessPort
from srs.ports.outbound.dao import (
    DataAccessCommitteeDao,
    DataAccessPolicyDao,
    DatasetDao,
)

log = logging.getLogger(__name__)


class DataAccessController(DataAccessPort):
    """Core implementation of DAC and DAP operations."""

    def __init__(
        self,
        *,
        dac_dao: DataAccessCommitteeDao,
        dap_dao: DataAccessPolicyDao,
        dataset_dao: DatasetDao,
    ):
        self._dac_dao = dac_dao
        self._dap_dao = dap_dao
        self._dataset_dao = dataset_dao

    # --- DAC operations ---

    async def create_dac(
        self,
        *,
        id: str,
        name: str,
        email: str,
        institute: str,
    ) -> None:
        """Create a new DAC."""
        today = now_as_utc()
        dac = DataAccessCommittee(
            id=id,
            name=name,
            email=email,
            institute=institute,
            created=today,
            changed=today,
            active=True,
        )
        try:
            await self._dac_dao.insert(dac)
        except Exception as err:
            raise self.DuplicateError(
                detail=f"DAC with ID {id} already exists."
            ) from err
        log.info("Created DAC %s", id)

    async def get_dacs(self) -> list[DataAccessCommittee]:
        """Get all DACs."""
        return [dac async for dac in self._dac_dao.find_all(mapping={})]

    async def get_dac(self, *, dac_id: str) -> DataAccessCommittee:
        """Get a DAC by ID."""
        try:
            return await self._dac_dao.get_by_id(dac_id)
        except ResourceNotFoundError as err:
            raise self.DacNotFoundError(dac_id=dac_id) from err

    async def update_dac(
        self,
        *,
        dac_id: str,
        name: str | None = None,
        email: str | None = None,
        institute: str | None = None,
        active: bool | None = None,
    ) -> None:
        """Update a DAC."""
        try:
            dac = await self._dac_dao.get_by_id(dac_id)
        except ResourceNotFoundError as err:
            raise self.DacNotFoundError(dac_id=dac_id) from err

        updates = {
            k: v
            for k, v in {
                "name": name,
                "email": email,
                "institute": institute,
                "active": active,
            }.items()
            if v is not None
        }
        updates["changed"] = now_as_utc()

        dac = dac.model_copy(update=updates)
        await self._dac_dao.update(dac)
        log.info("Updated DAC %s", dac_id)

    async def delete_dac(self, *, dac_id: str) -> None:
        """Delete a DAC."""
        try:
            await self._dac_dao.get_by_id(dac_id)
        except ResourceNotFoundError as err:
            raise self.DacNotFoundError(dac_id=dac_id) from err

        # Check for referencing DAPs
        async for dap in self._dap_dao.find_all(mapping={"dac_id": dac_id}):
            raise self.ReferenceConflictError(
                detail=f"Cannot delete DAC {dac_id}; "
                f"it is referenced by DAP {dap.id}."
            )

        await self._dac_dao.delete(dac_id)
        log.info("Deleted DAC %s", dac_id)

    # --- DAP operations ---

    async def create_dap(
        self,
        *,
        id: str,
        name: str,
        description: str,
        text: str,
        url: str | None,
        duo_permission_id: str,
        duo_modifier_ids: list[str],
        dac_id: str,
    ) -> None:
        """Create a new DAP."""
        # Verify DAC exists
        try:
            await self._dac_dao.get_by_id(dac_id)
        except ResourceNotFoundError as err:
            raise self.DacNotFoundError(dac_id=dac_id) from err

        today = now_as_utc()
        dap = DataAccessPolicy(
            id=id,
            name=name,
            description=description,
            text=text,
            url=url,
            duo_permission_id=DuoPermission(duo_permission_id),
            duo_modifier_ids=[DuoModifier(m) for m in duo_modifier_ids],
            dac_id=dac_id,
            created=today,
            changed=today,
            active=True,
        )
        try:
            await self._dap_dao.insert(dap)
        except Exception as err:
            raise self.DuplicateError(
                detail=f"DAP with ID {id} already exists."
            ) from err
        log.info("Created DAP %s", id)

    async def get_daps(self) -> list[DataAccessPolicy]:
        """Get all DAPs."""
        return [dap async for dap in self._dap_dao.find_all(mapping={})]

    async def get_dap(self, *, dap_id: str) -> DataAccessPolicy:
        """Get a DAP by ID."""
        try:
            return await self._dap_dao.get_by_id(dap_id)
        except ResourceNotFoundError as err:
            raise self.DapNotFoundError(dap_id=dap_id) from err

    async def update_dap(
        self,
        *,
        dap_id: str,
        name: str | None = None,
        description: str | None = None,
        text: str | None = None,
        url: str | None = None,
        duo_permission_id: str | None = None,
        duo_modifier_ids: list[str] | None = None,
        dac_id: str | None = None,
        active: bool | None = None,
    ) -> None:
        """Update a DAP."""
        try:
            dap = await self._dap_dao.get_by_id(dap_id)
        except ResourceNotFoundError as err:
            raise self.DapNotFoundError(dap_id=dap_id) from err

        if dac_id is not None:
            try:
                await self._dac_dao.get_by_id(dac_id)
            except ResourceNotFoundError as err:
                raise self.DacNotFoundError(dac_id=dac_id) from err

        updates = {
            k: v
            for k, v in {
                "name": name,
                "description": description,
                "text": text,
                "url": url,
                "duo_permission_id": (
                    DuoPermission(duo_permission_id)
                    if duo_permission_id is not None
                    else None
                ),
                "duo_modifier_ids": (
                    [DuoModifier(m) for m in duo_modifier_ids]
                    if duo_modifier_ids is not None
                    else None
                ),
                "dac_id": dac_id,
                "active": active,
            }.items()
            if v is not None
        }
        updates["changed"] = now_as_utc()

        dap = dap.model_copy(update=updates)
        await self._dap_dao.update(dap)
        log.info("Updated DAP %s", dap_id)

    async def delete_dap(self, *, dap_id: str) -> None:
        """Delete a DAP."""
        try:
            await self._dap_dao.get_by_id(dap_id)
        except ResourceNotFoundError as err:
            raise self.DapNotFoundError(dap_id=dap_id) from err

        # Check for referencing datasets
        async for ds in self._dataset_dao.find_all(mapping={"dap_id": dap_id}):
            raise self.ReferenceConflictError(
                detail=f"Cannot delete DAP {dap_id}; "
                f"it is referenced by dataset {ds.id}."
            )

        await self._dap_dao.delete(dap_id)
        log.info("Deleted DAP %s", dap_id)
