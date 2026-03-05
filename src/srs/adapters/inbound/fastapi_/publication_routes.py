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

"""FastAPI routes for Publication operations."""

import logging

from fastapi import APIRouter, Query, status

from srs.adapters.inbound.fastapi_ import dummies
from srs.adapters.inbound.fastapi_.http_authorization import (
    OptionalAuthContext,
    StewardAuthContext,
    get_optional_user_id,
    is_optional_data_steward,
)
from srs.adapters.inbound.fastapi_.http_exceptions import (
    HttpInternalError,
    HttpNotAuthorizedError,
    HttpPublicationNotFoundError,
    HttpStatusConflictError,
    HttpStudyNotFoundError,
)
from srs.adapters.inbound.fastapi_.rest_models import PublicationCreateRequest
from srs.core.models import Publication
from srs.ports.inbound.publication import PublicationPort

log = logging.getLogger(__name__)

publication_router = APIRouter(tags=["Publications"])

# ───────────────────────── Publications ─────────────────────────


@publication_router.post(
    "/studies/{study_id}/publications",
    summary="Create a publication",
    operation_id="createPublication",
    status_code=status.HTTP_201_CREATED,
    response_model=Publication,
)
async def create_publication(
    study_id: str,
    body: PublicationCreateRequest,
    auth: StewardAuthContext,
    registry: dummies.StudyRegistryDummy,
):
    """Add a publication to a study."""
    try:
        return await registry.publications.create_publication(
            data={**body.model_dump(), "study_id": study_id},
        )
    except PublicationPort.StudyNotFoundError as err:
        raise HttpStudyNotFoundError(study_id=study_id) from err
    except PublicationPort.StatusConflictError as err:
        raise HttpStatusConflictError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in create_publication")
        raise HttpInternalError() from err


@publication_router.get(
    "/publications",
    summary="List publications",
    operation_id="getPublications",
    response_model=list[Publication],
)
async def get_publications(
    registry: dummies.StudyRegistryDummy,
    auth: OptionalAuthContext = None,
    year: int | None = Query(None),
    text: str | None = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """Get a filtered list of publications."""
    try:
        return await registry.publications.get_publications(
            year=year,
            text=text,
            skip=skip,
            limit=limit,
            user_id=get_optional_user_id(auth),
            is_data_steward=is_optional_data_steward(auth),
        )
    except Exception as err:
        log.exception("Unexpected error in get_publications")
        raise HttpInternalError() from err


@publication_router.get(
    "/publications/{publication_id}",
    summary="Get a publication",
    operation_id="getPublication",
    response_model=Publication,
)
async def get_publication(
    publication_id: str,
    registry: dummies.StudyRegistryDummy,
    auth: OptionalAuthContext = None,
):
    """Get a single publication by its accession."""
    try:
        return await registry.publications.get_publication(
            publication_id=publication_id,
            user_id=get_optional_user_id(auth),
            is_data_steward=is_optional_data_steward(auth),
        )
    except PublicationPort.PublicationNotFoundError as err:
        raise HttpPublicationNotFoundError(publication_id=publication_id) from err
    except PublicationPort.AccessDeniedError as err:
        raise HttpNotAuthorizedError() from err
    except Exception as err:
        log.exception("Unexpected error in get_publication")
        raise HttpInternalError() from err


@publication_router.delete(
    "/publications/{publication_id}",
    summary="Delete a publication",
    operation_id="deletePublication",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_publication(
    publication_id: str,
    auth: StewardAuthContext,
    registry: dummies.StudyRegistryDummy,
):
    """Delete a publication."""
    try:
        await registry.publications.delete_publication(
            publication_id=publication_id
        )
    except PublicationPort.PublicationNotFoundError as err:
        raise HttpPublicationNotFoundError(publication_id=publication_id) from err
    except PublicationPort.StatusConflictError as err:
        raise HttpStatusConflictError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in delete_publication")
        raise HttpInternalError() from err
