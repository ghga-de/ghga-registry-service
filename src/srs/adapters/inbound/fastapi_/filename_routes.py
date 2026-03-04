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

"""FastAPI routes for Filename operations."""

import logging

from fastapi import APIRouter, status

from srs.adapters.inbound.fastapi_ import dummies
from srs.adapters.inbound.fastapi_.http_authorization import (
    StewardAuthContext,
)
from srs.adapters.inbound.fastapi_.http_exceptions import (
    HttpInternalError,
    HttpMetadataNotFoundError,
    HttpStudyNotFoundError,
    HttpValidationError,
)
from srs.adapters.inbound.fastapi_.rest_models import FileIdMappingRequest
from srs.constants import TRACER
from srs.ports.inbound.filename import FilenamePort

log = logging.getLogger(__name__)

filename_router = APIRouter(tags=["Filenames"])

# ──────────────────────── Filenames ─────────────────────────────


@filename_router.get(
    "/filenames/{study_id}",
    summary="Get file accession-to-name mapping",
    operation_id="getFilenames",
    response_model=dict[str, dict[str, str]],
)
@TRACER.start_as_current_span("routes.get_filenames")
async def get_filenames(
    study_id: str,
    auth: StewardAuthContext,
    registry: dummies.StudyRegistryDummy,
):
    """Get file accession to filename/alias mapping for a study."""
    try:
        return await registry.filenames.get_filenames(study_id=study_id)
    except FilenamePort.StudyNotFoundError as err:
        raise HttpStudyNotFoundError(study_id=study_id) from err
    except FilenamePort.MetadataNotFoundError as err:
        raise HttpMetadataNotFoundError(study_id=study_id) from err
    except Exception as err:
        log.exception("Unexpected error in get_filenames")
        raise HttpInternalError() from err


@filename_router.post(
    "/filenames/{study_id}",
    summary="Post file ID mappings",
    operation_id="postFilenames",
    status_code=status.HTTP_204_NO_CONTENT,
)
@TRACER.start_as_current_span("routes.post_filenames")
async def post_filenames(
    study_id: str,
    body: FileIdMappingRequest,
    auth: StewardAuthContext,
    registry: dummies.StudyRegistryDummy,
):
    """Store file accession to internal file ID mappings."""
    try:
        await registry.filenames.post_filenames(
            study_id=study_id, file_id_map=body.file_id_map
        )
    except FilenamePort.StudyNotFoundError as err:
        raise HttpStudyNotFoundError(study_id=study_id) from err
    except FilenamePort.ValidationError as err:
        raise HttpValidationError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in post_filenames")
        raise HttpInternalError() from err
