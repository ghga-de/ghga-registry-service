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

"""FastAPI routes for Datasets."""

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
    HttpDapNotFoundError,
    HttpDatasetNotFoundError,
    HttpInternalError,
    HttpNotAuthorizedError,
    HttpStatusConflictError,
    HttpStudyNotFoundError,
    HttpValidationError,
)
from srs.adapters.inbound.fastapi_.rest_models import (
    DatasetCreateRequest,
    DatasetUpdateRequest,
)
from srs.core.models import Dataset
from srs.ports.inbound.dataset import DatasetPort

log = logging.getLogger(__name__)

dataset_router = APIRouter(tags=["Datasets"])

# ──────────────────────────── Datasets ──────────────────────────


@dataset_router.post(
    "/studies/{study_id}/datasets",
    summary="Create a dataset",
    operation_id="createDataset",
    status_code=status.HTTP_201_CREATED,
    response_model=Dataset,
)
async def create_dataset(
    study_id: str,
    body: DatasetCreateRequest,
    auth: StewardAuthContext,
    registry: dummies.StudyRegistryDummy,
):
    """Create a dataset for a study."""
    try:
        return await registry.datasets.create_dataset(
            title=body.title,
            description=body.description,
            types=body.types,
            study_id=study_id,
            dap_id=body.dap_id,
            files=body.files,
        )
    except DatasetPort.StudyNotFoundError as err:
        raise HttpStudyNotFoundError(study_id=study_id) from err
    except DatasetPort.StatusConflictError as err:
        raise HttpStatusConflictError(detail=str(err)) from err
    except DatasetPort.DapNotFoundError as err:
        raise HttpDapNotFoundError(dap_id=body.dap_id) from err
    except DatasetPort.ValidationError as err:
        raise HttpValidationError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in create_dataset")
        raise HttpInternalError() from err


@dataset_router.get(
    "/datasets",
    summary="List datasets",
    operation_id="getDatasets",
    response_model=list[Dataset],
)
async def get_datasets(
    registry: dummies.StudyRegistryDummy,
    auth: OptionalAuthContext = None,
    dataset_type: str | None = Query(None),
    study_id: str | None = Query(None),
    text: str | None = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """Get a filtered list of datasets."""
    try:
        return await registry.datasets.get_datasets(
            dataset_type=dataset_type,
            study_id=study_id,
            text=text,
            skip=skip,
            limit=limit,
            user_id=get_optional_user_id(auth),
            is_data_steward=is_optional_data_steward(auth),
        )
    except Exception as err:
        log.exception("Unexpected error in get_datasets")
        raise HttpInternalError() from err


@dataset_router.get(
    "/datasets/{dataset_id}",
    summary="Get a dataset",
    operation_id="getDataset",
    response_model=Dataset,
)
async def get_dataset(
    dataset_id: str,
    registry: dummies.StudyRegistryDummy,
    auth: OptionalAuthContext = None,
):
    """Get a dataset by its accession ID."""
    try:
        return await registry.datasets.get_dataset(
            dataset_id=dataset_id,
            user_id=get_optional_user_id(auth),
            is_data_steward=is_optional_data_steward(auth),
        )
    except DatasetPort.DatasetNotFoundError as err:
        raise HttpDatasetNotFoundError(dataset_id=dataset_id) from err
    except DatasetPort.AccessDeniedError as err:
        raise HttpNotAuthorizedError() from err
    except Exception as err:
        log.exception("Unexpected error in get_dataset")
        raise HttpInternalError() from err


@dataset_router.patch(
    "/datasets/{dataset_id}",
    summary="Update a dataset",
    operation_id="updateDataset",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def update_dataset(
    dataset_id: str,
    body: DatasetUpdateRequest,
    auth: StewardAuthContext,
    registry: dummies.StudyRegistryDummy,
):
    """Update the DAP assignment for a dataset."""
    try:
        await registry.datasets.update_dataset(
            dataset_id=dataset_id, dap_id=body.dap_id
        )
    except DatasetPort.DatasetNotFoundError as err:
        raise HttpDatasetNotFoundError(dataset_id=dataset_id) from err
    except DatasetPort.DapNotFoundError as err:
        raise HttpDapNotFoundError(dap_id=body.dap_id) from err
    except Exception as err:
        log.exception("Unexpected error in update_dataset")
        raise HttpInternalError() from err


@dataset_router.delete(
    "/datasets/{dataset_id}",
    summary="Delete a dataset",
    operation_id="deleteDataset",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_dataset(
    dataset_id: str,
    auth: StewardAuthContext,
    registry: dummies.StudyRegistryDummy,
):
    """Delete a dataset."""
    try:
        await registry.datasets.delete_dataset(dataset_id=dataset_id)
    except DatasetPort.DatasetNotFoundError as err:
        raise HttpDatasetNotFoundError(dataset_id=dataset_id) from err
    except DatasetPort.StatusConflictError as err:
        raise HttpStatusConflictError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in delete_dataset")
        raise HttpInternalError() from err
