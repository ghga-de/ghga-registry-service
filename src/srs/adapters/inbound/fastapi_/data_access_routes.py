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

"""FastAPI routes for Data Access Committees and Data Access Policies."""

import logging

from fastapi import APIRouter, status

from srs.adapters.inbound.fastapi_ import dummies
from srs.adapters.inbound.fastapi_.http_authorization import StewardAuthContext
from srs.adapters.inbound.fastapi_.http_exceptions import (
    HttpDacNotFoundError,
    HttpDapNotFoundError,
    HttpDuplicateError,
    HttpInternalError,
    HttpReferenceConflictError,
)
from srs.adapters.inbound.fastapi_.rest_models import (
    DacCreateRequest,
    DacUpdateRequest,
    DapCreateRequest,
    DapUpdateRequest,
)
from srs.core.models import DataAccessCommittee, DataAccessPolicy
from srs.ports.inbound.data_access import DataAccessPort

log = logging.getLogger(__name__)

data_access_router = APIRouter(tags=["Data Access"])

# ──────────────────── Data Access Committees ────────────────────


@data_access_router.post(
    "/dacs",
    summary="Create a DAC",
    operation_id="createDac",
    status_code=status.HTTP_201_CREATED,
)
async def create_dac(
    body: DacCreateRequest,
    auth: StewardAuthContext,
    registry: dummies.StudyRegistryDummy,
):
    """Create a new data access committee."""
    try:
        await registry.data_access.create_dac(
            id=body.id,
            name=body.name,
            email=body.email,
            institute=body.institute,
        )
    except DataAccessPort.DuplicateError as err:
        raise HttpDuplicateError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in create_dac")
        raise HttpInternalError() from err


@data_access_router.get(
    "/dacs",
    summary="List DACs",
    operation_id="getDacs",
    response_model=list[DataAccessCommittee],
)
async def get_dacs(
    registry: dummies.StudyRegistryDummy,
):
    """Get all data access committees."""
    try:
        return await registry.data_access.get_dacs()
    except Exception as err:
        log.exception("Unexpected error in get_dacs")
        raise HttpInternalError() from err


@data_access_router.get(
    "/dacs/{dac_id}",
    summary="Get a DAC",
    operation_id="getDac",
    response_model=DataAccessCommittee,
)
async def get_dac(
    dac_id: str,
    registry: dummies.StudyRegistryDummy,
):
    """Get a data access committee by ID."""
    try:
        return await registry.data_access.get_dac(dac_id=dac_id)
    except DataAccessPort.DacNotFoundError as err:
        raise HttpDacNotFoundError(dac_id=dac_id) from err
    except Exception as err:
        log.exception("Unexpected error in get_dac")
        raise HttpInternalError() from err


@data_access_router.patch(
    "/dacs/{dac_id}",
    summary="Update a DAC",
    operation_id="updateDac",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def update_dac(
    dac_id: str,
    body: DacUpdateRequest,
    auth: StewardAuthContext,
    registry: dummies.StudyRegistryDummy,
):
    """Update a data access committee."""
    try:
        await registry.data_access.update_dac(
            dac_id=dac_id,
            name=body.name,
            email=body.email,
            institute=body.institute,
            active=body.active,
        )
    except DataAccessPort.DacNotFoundError as err:
        raise HttpDacNotFoundError(dac_id=dac_id) from err
    except Exception as err:
        log.exception("Unexpected error in update_dac")
        raise HttpInternalError() from err


@data_access_router.delete(
    "/dacs/{dac_id}",
    summary="Delete a DAC",
    operation_id="deleteDac",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_dac(
    dac_id: str,
    auth: StewardAuthContext,
    registry: dummies.StudyRegistryDummy,
):
    """Delete a data access committee."""
    try:
        await registry.data_access.delete_dac(dac_id=dac_id)
    except DataAccessPort.DacNotFoundError as err:
        raise HttpDacNotFoundError(dac_id=dac_id) from err
    except DataAccessPort.ReferenceConflictError as err:
        raise HttpReferenceConflictError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in delete_dac")
        raise HttpInternalError() from err


# ──────────────────── Data Access Policies ──────────────────────


@data_access_router.post(
    "/daps",
    summary="Create a DAP",
    operation_id="createDap",
    status_code=status.HTTP_201_CREATED,
)
async def create_dap(
    body: DapCreateRequest,
    auth: StewardAuthContext,
    registry: dummies.StudyRegistryDummy,
):
    """Create a new data access policy."""
    try:
        await registry.data_access.create_dap(
            id=body.id,
            name=body.name,
            description=body.description,
            text=body.text,
            url=body.url,
            duo_permission_id=body.duo_permission_id,
            duo_modifier_ids=body.duo_modifier_ids,
            dac_id=body.dac_id,
        )
    except DataAccessPort.DacNotFoundError as err:
        raise HttpDacNotFoundError(dac_id=body.dac_id) from err
    except DataAccessPort.DuplicateError as err:
        raise HttpDuplicateError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in create_dap")
        raise HttpInternalError() from err


@data_access_router.get(
    "/daps",
    summary="List DAPs",
    operation_id="getDaps",
    response_model=list[DataAccessPolicy],
)
async def get_daps(
    registry: dummies.StudyRegistryDummy,
):
    """Get all data access policies."""
    try:
        return await registry.data_access.get_daps()
    except Exception as err:
        log.exception("Unexpected error in get_daps")
        raise HttpInternalError() from err


@data_access_router.get(
    "/daps/{dap_id}",
    summary="Get a DAP",
    operation_id="getDap",
    response_model=DataAccessPolicy,
)
async def get_dap(
    dap_id: str,
    registry: dummies.StudyRegistryDummy,
):
    """Get a data access policy by ID."""
    try:
        return await registry.data_access.get_dap(dap_id=dap_id)
    except DataAccessPort.DapNotFoundError as err:
        raise HttpDapNotFoundError(dap_id=dap_id) from err
    except Exception as err:
        log.exception("Unexpected error in get_dap")
        raise HttpInternalError() from err


@data_access_router.patch(
    "/daps/{dap_id}",
    summary="Update a DAP",
    operation_id="updateDap",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def update_dap(
    dap_id: str,
    body: DapUpdateRequest,
    auth: StewardAuthContext,
    registry: dummies.StudyRegistryDummy,
):
    """Update a data access policy."""
    try:
        await registry.data_access.update_dap(
            dap_id=dap_id,
            name=body.name,
            description=body.description,
            text=body.text,
            url=body.url,
            duo_permission_id=body.duo_permission_id,
            duo_modifier_ids=body.duo_modifier_ids,
            dac_id=body.dac_id,
            active=body.active,
        )
    except DataAccessPort.DapNotFoundError as err:
        raise HttpDapNotFoundError(dap_id=dap_id) from err
    except DataAccessPort.DacNotFoundError as err:
        raise HttpDacNotFoundError(dac_id=body.dac_id or "") from err
    except Exception as err:
        log.exception("Unexpected error in update_dap")
        raise HttpInternalError() from err


@data_access_router.delete(
    "/daps/{dap_id}",
    summary="Delete a DAP",
    operation_id="deleteDap",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_dap(
    dap_id: str,
    auth: StewardAuthContext,
    registry: dummies.StudyRegistryDummy,
):
    """Delete a data access policy."""
    try:
        await registry.data_access.delete_dap(dap_id=dap_id)
    except DataAccessPort.DapNotFoundError as err:
        raise HttpDapNotFoundError(dap_id=dap_id) from err
    except DataAccessPort.ReferenceConflictError as err:
        raise HttpReferenceConflictError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in delete_dap")
        raise HttpInternalError() from err
