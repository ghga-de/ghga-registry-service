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

"""FastAPI endpoints for RS interaction"""

import logging
from enum import Enum
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query, status
from pydantic import UUID4

from rs.adapters.inbound.fastapi_.auth import StewardAuthContext
from rs.adapters.inbound.fastapi_.dummies import StudyRegistryDummy
from rs.adapters.inbound.fastapi_.http_exceptions import (
    HttpGrantNotFoundError,
    HttpInternalError,
)
from rs.adapters.inbound.fastapi_.routers.upload_boxes import box_router
from rs.constants import TRACER
from rs.core.models import GrantAccessRequest, GrantId, GrantWithBoxInfo
from rs.ports.inbound.orchestrator import UploadOrchestratorPort

log = logging.getLogger(__name__)

router = APIRouter()

TAGS: list[str | Enum] = ["ResearchDataUploadBoxes"]

router.include_router(
    box_router, prefix="/upload-boxes", tags=["ResearchDataUploadBoxes"]
)


@router.get(
    "/health",
    summary="health",
    status_code=200,
)
@TRACER.start_as_current_span("routes.health")
async def health():
    """Used to test if this service is alive"""
    return {"status": "OK"}


@router.post(
    "/upload-grants",
    summary="Grant upload access",
    description="Grant upload access to a user for a single research data upload box."
    + " Users cannot upload any files until they have been granted access to a box.",
    tags=TAGS,
    status_code=status.HTTP_201_CREATED,
    responses={
        201: {"description": "Upload access granted successfully."},
        401: {"description": "Not authenticated."},
        403: {"description": "Not authorized."},
        422: {"description": "Validation error in request body."},
    },
)
@TRACER.start_as_current_span("routes.grant_upload_access")
async def grant_upload_access(
    request: GrantAccessRequest,
    study_registry: StudyRegistryDummy,
    auth_context: StewardAuthContext,
) -> GrantId:
    """Grant upload access to a user. Requires Data Steward role."""
    try:
        return await study_registry.upload_orchestrator.grant_upload_access(
            user_id=request.user_id,
            iva_id=request.iva_id,
            box_id=request.box_id,
            valid_from=request.valid_from,
            valid_until=request.valid_until,
            granting_user_id=UUID(auth_context.id),
        )
    except Exception as err:
        log.error(err, exc_info=True)
        raise HttpInternalError(message="Failed to grant upload access") from err


@router.delete(
    "/upload-grants/{grant_id}",
    summary="Revoke an upload access grant",
    description="Revokes an existing upload access grant.",
    tags=TAGS,
    responses={
        204: {
            "description": "Upload access grant has been revoked.",
        },
        401: {"description": "Not authenticated."},
        403: {"description": "Not authorized."},
        404: {"description": "The upload access grant was not found."},
    },
    status_code=204,
)
@TRACER.start_as_current_span("routes.revoke_upload_access_grant")
async def revoke_upload_access_grant(
    grant_id: UUID4,
    study_registry: StudyRegistryDummy,
    auth_context: StewardAuthContext,
) -> None:
    """Revoke an upload access grant."""
    try:
        await study_registry.upload_orchestrator.revoke_upload_access_grant(grant_id)
    except UploadOrchestratorPort.GrantNotFoundError as err:
        raise HttpGrantNotFoundError(grant_id=grant_id) from err
    except Exception as err:
        log.error(err, exc_info=True)
        raise HttpInternalError(message="Failed to revoke access grant") from err


@router.get(
    "/upload-grants",
    summary="Get upload access grants",
    description=(
        "Endpoint to get the list of all upload access grants. Can be filtered by user"
        + " ID, IVA ID, and box ID. Results are sorted by validity, box ID, user ID,"
        + " IVA ID, and grant ID."
    ),
    tags=TAGS,
    responses={
        200: {
            "model": list[GrantWithBoxInfo],
            "description": "Upload access grants have been fetched.",
        },
        401: {"description": "Not authenticated."},
        403: {"description": "Not authorized."},
    },
    status_code=200,
)
@TRACER.start_as_current_span("routes.get_upload_access_grants")
async def get_upload_access_grants(  # noqa: PLR0913
    study_registry: StudyRegistryDummy,
    auth_context: StewardAuthContext,
    user_id: Annotated[
        UUID4 | None,
        Query(
            ...,
            alias="user_id",
            description="The internal ID of the user",
        ),
    ] = None,
    iva_id: Annotated[
        UUID4 | None,
        Query(
            ...,
            alias="iva_id",
            description="The ID of the IVA",
        ),
    ] = None,
    box_id: Annotated[
        UUID4 | None,
        Query(
            ...,
            alias="box_id",
            description="The ID of the upload box",
        ),
    ] = None,
    valid: Annotated[
        bool | None,
        Query(
            ...,
            alias="valid",
            description="Whether the grant is currently valid",
        ),
    ] = None,
) -> list[GrantWithBoxInfo]:
    """Get upload access grants.

    You can filter the grants by user ID, IVA ID, and box ID
    and by whether the grant is currently valid or not. Results are sorted by validity,
    user ID, IVA ID, box ID, and grant ID.
    """
    try:
        return await study_registry.upload_orchestrator.get_upload_access_grants(
            user_id=user_id, iva_id=iva_id, box_id=box_id, valid=valid
        )
    except Exception as err:
        log.error(err, exc_info=True)
        raise HttpInternalError(message="Failed to get upload access grants") from err


# TODO: Add `/filenames/{study_id}` endpoint to get ACCESSION -> name & alias mapping for files
