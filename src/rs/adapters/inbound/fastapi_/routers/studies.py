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

"""Routes related to Studies"""

import logging
from typing import Annotated

from fastapi import APIRouter, Query

from rs.adapters.inbound.fastapi_ import dummies
from rs.adapters.inbound.fastapi_.auth import StewardAuthContext
from rs.adapters.inbound.fastapi_.http_exceptions import (
    HttpInternalError,
    HttpStudyNotFoundError,
)
from rs.constants import TRACER
from rs.core.models import Study
from rs.ports.inbound.registry import RegistryPort

log = logging.getLogger(__name__)

study_router = APIRouter()


@study_router.get(
    "/{study_id}",
    summary="Get study details",
    description="Returns the details of a single study by its ID.",
    response_model=Study,
    responses={
        200: {
            "model": Study,
            "description": "Study details successfully retrieved.",
        },
        401: {"description": "Not authenticated."},
        403: {"description": "Not authorized."},
        404: {"description": "Study not found."},
    },
)
@TRACER.start_as_current_span("routes.get_study")
async def get_study(
    study_id: str,
    registry: dummies.RegistryDummy,
    auth_context: StewardAuthContext,
) -> Study:
    """Get details of a single study by its ID."""
    try:
        return await registry.get_study(study_id)
    except RegistryPort.StudyNotFoundError as err:
        raise HttpStudyNotFoundError(study_id=study_id) from err
    except Exception as err:
        log.error(err, exc_info=True)
        raise HttpInternalError(message="Failed to get study") from err


@study_router.get(
    "",
    summary="List studies",
    description=(
        "Returns the list of all studies, sorted by study ID. When"
        + " `with_unmapped_files` is set, only studies that still have associated file"
        + " accessions without an internal file ID (i.e. not yet mapped) are returned."
    ),
    response_model=list[Study],
    responses={
        200: {
            "model": list[Study],
            "description": "Studies successfully retrieved.",
        },
        401: {"description": "Not authenticated."},
        403: {"description": "Not authorized."},
        422: {"description": "Validation error in query parameters."},
    },
)
@TRACER.start_as_current_span("routes.get_studies")
async def get_studies(
    registry: dummies.RegistryDummy,
    auth_context: StewardAuthContext,
    with_unmapped_files: Annotated[
        bool,
        Query(
            description="Only return studies that have associated file accessions"
            " which have not been mapped to an internal file ID yet.",
        ),
    ] = False,
) -> list[Study]:
    """Get the list of all studies, optionally filtered to those with unmapped files."""
    try:
        return await registry.get_studies(with_unmapped_files=with_unmapped_files)
    except Exception as err:
        log.error(err, exc_info=True)
        raise HttpInternalError(message="Failed to get studies") from err
