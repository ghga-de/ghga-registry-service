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

"""FastAPI routes for Studies and Publish."""

import logging

from fastapi import APIRouter, Query, status

from srs.adapters.inbound.fastapi_ import dummies
from srs.adapters.inbound.fastapi_.http_authorization import (
    OptionalAuthContext,
    StewardAuthContext,
    get_optional_user_id,
    get_user_id,
    is_optional_data_steward,
)
from srs.adapters.inbound.fastapi_.http_exceptions import (
    HttpDuplicateError,
    HttpInternalError,
    HttpNotAuthorizedError,
    HttpStatusConflictError,
    HttpStudyNotFoundError,
    HttpValidationError,
)
from srs.adapters.inbound.fastapi_.rest_models import (
    StudyCreateRequest,
    StudyUpdateRequest,
)
from srs.constants import TRACER
from srs.core.models import (
    Study,
    StudyStatus,
)
from srs.ports.inbound.study import StudyPort

log = logging.getLogger(__name__)

study_router = APIRouter(tags=["Studies"])

# ──────────────────────────── Studies ────────────────────────────


@study_router.post(
    "/studies",
    summary="Create a new study",
    operation_id="createStudy",
    status_code=status.HTTP_201_CREATED,
    response_model=Study,
)
async def create_study(
    body: StudyCreateRequest,
    auth: StewardAuthContext,
    registry: dummies.StudyRegistryDummy,
):
    """Create a new study with status PENDING."""
    try:
        return await registry.studies.create_study(
            title=body.title,
            description=body.description,
            types=body.types,
            affiliations=body.affiliations,
            created_by=get_user_id(auth),
        )
    except StudyPort.StudyError as err:
        raise HttpDuplicateError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in create_study")
        raise HttpInternalError() from err


@study_router.get(
    "/studies",
    summary="List studies",
    operation_id="getStudies",
    response_model=list[Study],
)
async def get_studies(
    registry: dummies.StudyRegistryDummy,
    auth: OptionalAuthContext = None,
    status_filter: StudyStatus | None = Query(None, alias="status"),
    study_type: str | None = Query(None),
    text: str | None = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """Get a filtered list of studies."""
    try:
        return await registry.studies.get_studies(
            status=status_filter,
            study_type=study_type,
            text=text,
            skip=skip,
            limit=limit,
            user_id=get_optional_user_id(auth),
            is_data_steward=is_optional_data_steward(auth),
        )
    except Exception as err:
        log.exception("Unexpected error in get_studies")
        raise HttpInternalError() from err


@study_router.get(
    "/studies/{study_id}",
    summary="Get a study by ID",
    operation_id="getStudy",
    response_model=Study,
)
async def get_study(
    study_id: str,
    registry: dummies.StudyRegistryDummy,
    auth: OptionalAuthContext = None,
):
    """Get a single study by its accession ID."""
    try:
        return await registry.studies.get_study(
            study_id=study_id,
            user_id=get_optional_user_id(auth),
            is_data_steward=is_optional_data_steward(auth),
        )
    except StudyPort.StudyNotFoundError as err:
        raise HttpStudyNotFoundError(study_id=study_id) from err
    except StudyPort.AccessDeniedError as err:
        raise HttpNotAuthorizedError() from err
    except Exception as err:
        log.exception("Unexpected error in get_study")
        raise HttpInternalError() from err


@study_router.patch(
    "/studies/{study_id}",
    summary="Update a study",
    operation_id="updateStudy",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def update_study(
    study_id: str,
    body: StudyUpdateRequest,
    auth: StewardAuthContext,
    registry: dummies.StudyRegistryDummy,
):
    """Update study status and/or user list."""
    try:
        study_status = StudyStatus(body.status) if body.status else None
        await registry.studies.update_study(
            study_id=study_id,
            status=study_status,
            users=body.users,
            approved_by=body.approved_by,
        )
    except StudyPort.StudyNotFoundError as err:
        raise HttpStudyNotFoundError(study_id=study_id) from err
    except StudyPort.StatusConflictError as err:
        raise HttpStatusConflictError(detail=str(err)) from err
    except StudyPort.ValidationError as err:
        raise HttpValidationError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in update_study")
        raise HttpInternalError() from err


@study_router.delete(
    "/studies/{study_id}",
    summary="Delete a study",
    operation_id="deleteStudy",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_study(
    study_id: str,
    auth: StewardAuthContext,
    registry: dummies.StudyRegistryDummy,
):
    """Delete a study and all related entities."""
    try:
        await registry.studies.delete_study(study_id=study_id)
    except StudyPort.StudyNotFoundError as err:
        raise HttpStudyNotFoundError(study_id=study_id) from err
    except StudyPort.StatusConflictError as err:
        raise HttpStatusConflictError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in delete_study")
        raise HttpInternalError() from err


# ──────────────────────── Publish ───────────────────────────────


@study_router.post(
    "/studies/{study_id}/publish",
    summary="Publish a study",
    operation_id="publishStudy",
    status_code=status.HTTP_202_ACCEPTED,
)
@TRACER.start_as_current_span("routes.publish_study")
async def publish_study(
    study_id: str,
    auth: StewardAuthContext,
    registry: dummies.StudyRegistryDummy,
):
    """Validate and publish a study's annotated experimental metadata."""
    try:
        await registry.studies.publish_study(study_id=study_id)
    except StudyPort.StudyNotFoundError as err:
        raise HttpStudyNotFoundError(study_id=study_id) from err
    except StudyPort.ValidationError as err:
        raise HttpValidationError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in publish_study")
        raise HttpInternalError() from err
