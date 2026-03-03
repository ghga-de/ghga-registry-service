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

"""FastAPI routes for the Study Registry Service."""

import logging
from uuid import UUID

from fastapi import APIRouter, Query, status

from srs.adapters.inbound.fastapi_ import dummies
from srs.adapters.inbound.fastapi_.http_authorization import (
    AuthContextDep,
    OptionalAuthContextDep,
    get_optional_user_id,
    get_user_id,
    is_optional_data_steward,
    require_steward,
)
from srs.adapters.inbound.fastapi_.http_exceptions import (
    HttpAccessionNotFoundError,
    HttpDacNotFoundError,
    HttpDapNotFoundError,
    HttpDatasetNotFoundError,
    HttpDuplicateError,
    HttpInternalError,
    HttpMetadataNotFoundError,
    HttpNotAuthorizedError,
    HttpPublicationNotFoundError,
    HttpReferenceConflictError,
    HttpResourceTypeNotFoundError,
    HttpStatusConflictError,
    HttpStudyNotFoundError,
    HttpValidationError,
)
from srs.adapters.inbound.fastapi_.rest_models import (
    DacCreateRequest,
    DacUpdateRequest,
    DapCreateRequest,
    DapUpdateRequest,
    DatasetCreateRequest,
    DatasetUpdateRequest,
    FileIdMappingRequest,
    MetadataUpsertRequest,
    PublicationCreateRequest,
    ResourceTypeCreateRequest,
    ResourceTypeUpdateRequest,
    StudyCreateRequest,
    StudyUpdateRequest,
)
from srs.constants import TRACER
from srs.core.models import (
    Accession,
    AltAccession,
    AltAccessionType,
    DataAccessCommittee,
    DataAccessPolicy,
    Dataset,
    ExperimentalMetadata,
    Publication,
    ResourceType,
    Study,
    StudyStatus,
    TypedResource,
)
from srs.ports.inbound.study_registry import StudyRegistryPort

log = logging.getLogger(__name__)

router = APIRouter(tags=["Study Registry Service"])

# ──────────────────────────── Studies ────────────────────────────


@router.post(
    "/studies",
    summary="Create a new study",
    operation_id="createStudy",
    status_code=status.HTTP_201_CREATED,
    response_model=Study,
)
@TRACER.start_as_current_span("routes.create_study")
async def create_study(
    body: StudyCreateRequest,
    auth: AuthContextDep,
    registry: dummies.StudyRegistryDummy,
):
    """Create a new study with status PENDING."""
    require_steward(auth)
    try:
        return await registry.create_study(
            title=body.title,
            description=body.description,
            types=body.types,
            affiliations=body.affiliations,
            created_by=get_user_id(auth),
        )
    except StudyRegistryPort.DuplicateError as err:
        raise HttpDuplicateError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in create_study")
        raise HttpInternalError() from err


@router.get(
    "/studies",
    summary="List studies",
    operation_id="getStudies",
    response_model=list[Study],
)
@TRACER.start_as_current_span("routes.get_studies")
async def get_studies(
    registry: dummies.StudyRegistryDummy,
    auth: OptionalAuthContextDep = None,
    status_filter: StudyStatus | None = Query(None, alias="status"),
    study_type: str | None = Query(None),
    text: str | None = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """Get a filtered list of studies."""
    try:
        return await registry.get_studies(
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


@router.get(
    "/studies/{study_id}",
    summary="Get a study by ID",
    operation_id="getStudy",
    response_model=Study,
)
@TRACER.start_as_current_span("routes.get_study")
async def get_study(
    study_id: str,
    registry: dummies.StudyRegistryDummy,
    auth: OptionalAuthContextDep = None,
):
    """Get a single study by its accession ID."""
    try:
        return await registry.get_study(
            study_id=study_id,
            user_id=get_optional_user_id(auth),
            is_data_steward=is_optional_data_steward(auth),
        )
    except StudyRegistryPort.StudyNotFoundError as err:
        raise HttpStudyNotFoundError(study_id=study_id) from err
    except StudyRegistryPort.AccessDeniedError as err:
        raise HttpNotAuthorizedError() from err
    except Exception as err:
        log.exception("Unexpected error in get_study")
        raise HttpInternalError() from err


@router.patch(
    "/studies/{study_id}",
    summary="Update a study",
    operation_id="updateStudy",
    status_code=status.HTTP_204_NO_CONTENT,
)
@TRACER.start_as_current_span("routes.update_study")
async def update_study(
    study_id: str,
    body: StudyUpdateRequest,
    auth: AuthContextDep,
    registry: dummies.StudyRegistryDummy,
):
    """Update study status and/or user list."""
    require_steward(auth)
    try:
        study_status = StudyStatus(body.status) if body.status else None
        await registry.update_study(
            study_id=study_id,
            status=study_status,
            users=body.users,
            approved_by=body.approved_by,
        )
    except StudyRegistryPort.StudyNotFoundError as err:
        raise HttpStudyNotFoundError(study_id=study_id) from err
    except StudyRegistryPort.StatusConflictError as err:
        raise HttpStatusConflictError(detail=str(err)) from err
    except StudyRegistryPort.ValidationError as err:
        raise HttpValidationError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in update_study")
        raise HttpInternalError() from err


@router.delete(
    "/studies/{study_id}",
    summary="Delete a study",
    operation_id="deleteStudy",
    status_code=status.HTTP_204_NO_CONTENT,
)
@TRACER.start_as_current_span("routes.delete_study")
async def delete_study(
    study_id: str,
    auth: AuthContextDep,
    registry: dummies.StudyRegistryDummy,
):
    """Delete a study and all related entities."""
    require_steward(auth)
    try:
        await registry.delete_study(study_id=study_id)
    except StudyRegistryPort.StudyNotFoundError as err:
        raise HttpStudyNotFoundError(study_id=study_id) from err
    except StudyRegistryPort.StatusConflictError as err:
        raise HttpStatusConflictError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in delete_study")
        raise HttpInternalError() from err


# ──────────────────── Experimental Metadata ─────────────────────


@router.put(
    "/studies/{study_id}/metadata",
    summary="Upsert experimental metadata",
    operation_id="upsertMetadata",
    status_code=status.HTTP_204_NO_CONTENT,
)
@TRACER.start_as_current_span("routes.upsert_metadata")
async def upsert_metadata(
    study_id: str,
    body: MetadataUpsertRequest,
    auth: AuthContextDep,
    registry: dummies.StudyRegistryDummy,
):
    """Create or replace experimental metadata for a study."""
    require_steward(auth)
    try:
        await registry.upsert_metadata(
            study_id=study_id, metadata=body.metadata
        )
    except StudyRegistryPort.StudyNotFoundError as err:
        raise HttpStudyNotFoundError(study_id=study_id) from err
    except StudyRegistryPort.StatusConflictError as err:
        raise HttpStatusConflictError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in upsert_metadata")
        raise HttpInternalError() from err


@router.get(
    "/studies/{study_id}/metadata",
    summary="Get experimental metadata",
    operation_id="getMetadata",
    response_model=ExperimentalMetadata,
)
@TRACER.start_as_current_span("routes.get_metadata")
async def get_metadata(
    study_id: str,
    auth: AuthContextDep,
    registry: dummies.StudyRegistryDummy,
):
    """Get experimental metadata for a study."""
    require_steward(auth)
    try:
        return await registry.get_metadata(study_id=study_id)
    except StudyRegistryPort.MetadataNotFoundError as err:
        raise HttpMetadataNotFoundError(study_id=study_id) from err
    except Exception as err:
        log.exception("Unexpected error in get_metadata")
        raise HttpInternalError() from err


@router.delete(
    "/studies/{study_id}/metadata",
    summary="Delete experimental metadata",
    operation_id="deleteMetadata",
    status_code=status.HTTP_204_NO_CONTENT,
)
@TRACER.start_as_current_span("routes.delete_metadata")
async def delete_metadata(
    study_id: str,
    auth: AuthContextDep,
    registry: dummies.StudyRegistryDummy,
):
    """Delete experimental metadata for a study."""
    require_steward(auth)
    try:
        await registry.delete_metadata(study_id=study_id)
    except StudyRegistryPort.StudyNotFoundError as err:
        raise HttpStudyNotFoundError(study_id=study_id) from err
    except StudyRegistryPort.StatusConflictError as err:
        raise HttpStatusConflictError(detail=str(err)) from err
    except StudyRegistryPort.MetadataNotFoundError as err:
        raise HttpMetadataNotFoundError(study_id=study_id) from err
    except Exception as err:
        log.exception("Unexpected error in delete_metadata")
        raise HttpInternalError() from err


# ───────────────────────── Publications ─────────────────────────


@router.post(
    "/studies/{study_id}/publications",
    summary="Create a publication",
    operation_id="createPublication",
    status_code=status.HTTP_201_CREATED,
    response_model=Publication,
)
@TRACER.start_as_current_span("routes.create_publication")
async def create_publication(
    study_id: str,
    body: PublicationCreateRequest,
    auth: AuthContextDep,
    registry: dummies.StudyRegistryDummy,
):
    """Add a publication to a study."""
    require_steward(auth)
    try:
        return await registry.create_publication(
            title=body.title,
            abstract=body.abstract,
            authors=body.authors,
            year=body.year,
            journal=body.journal,
            doi=body.doi,
            study_id=study_id,
        )
    except StudyRegistryPort.StudyNotFoundError as err:
        raise HttpStudyNotFoundError(study_id=study_id) from err
    except StudyRegistryPort.StatusConflictError as err:
        raise HttpStatusConflictError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in create_publication")
        raise HttpInternalError() from err


@router.get(
    "/publications",
    summary="List publications",
    operation_id="getPublications",
    response_model=list[Publication],
)
@TRACER.start_as_current_span("routes.get_publications")
async def get_publications(
    registry: dummies.StudyRegistryDummy,
    auth: OptionalAuthContextDep = None,
    year: int | None = Query(None),
    text: str | None = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """Get a filtered list of publications."""
    try:
        return await registry.get_publications(
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


@router.get(
    "/publications/{publication_id}",
    summary="Get a publication",
    operation_id="getPublication",
    response_model=Publication,
)
@TRACER.start_as_current_span("routes.get_publication")
async def get_publication(
    publication_id: str,
    registry: dummies.StudyRegistryDummy,
    auth: OptionalAuthContextDep = None,
):
    """Get a single publication by its accession."""
    try:
        return await registry.get_publication(
            publication_id=publication_id,
            user_id=get_optional_user_id(auth),
            is_data_steward=is_optional_data_steward(auth),
        )
    except StudyRegistryPort.PublicationNotFoundError as err:
        raise HttpPublicationNotFoundError(publication_id=publication_id) from err
    except StudyRegistryPort.AccessDeniedError as err:
        raise HttpNotAuthorizedError() from err
    except Exception as err:
        log.exception("Unexpected error in get_publication")
        raise HttpInternalError() from err


@router.delete(
    "/publications/{publication_id}",
    summary="Delete a publication",
    operation_id="deletePublication",
    status_code=status.HTTP_204_NO_CONTENT,
)
@TRACER.start_as_current_span("routes.delete_publication")
async def delete_publication(
    publication_id: str,
    auth: AuthContextDep,
    registry: dummies.StudyRegistryDummy,
):
    """Delete a publication."""
    require_steward(auth)
    try:
        await registry.delete_publication(publication_id=publication_id)
    except StudyRegistryPort.PublicationNotFoundError as err:
        raise HttpPublicationNotFoundError(publication_id=publication_id) from err
    except StudyRegistryPort.StatusConflictError as err:
        raise HttpStatusConflictError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in delete_publication")
        raise HttpInternalError() from err


# ──────────────────── Data Access Committees ────────────────────


@router.post(
    "/dacs",
    summary="Create a DAC",
    operation_id="createDac",
    status_code=status.HTTP_201_CREATED,
)
@TRACER.start_as_current_span("routes.create_dac")
async def create_dac(
    body: DacCreateRequest,
    auth: AuthContextDep,
    registry: dummies.StudyRegistryDummy,
):
    """Create a new data access committee."""
    require_steward(auth)
    try:
        await registry.create_dac(
            id=body.id,
            name=body.name,
            email=body.email,
            institute=body.institute,
        )
    except StudyRegistryPort.DuplicateError as err:
        raise HttpDuplicateError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in create_dac")
        raise HttpInternalError() from err


@router.get(
    "/dacs",
    summary="List DACs",
    operation_id="getDacs",
    response_model=list[DataAccessCommittee],
)
@TRACER.start_as_current_span("routes.get_dacs")
async def get_dacs(
    registry: dummies.StudyRegistryDummy,
):
    """Get all data access committees."""
    try:
        return await registry.get_dacs()
    except Exception as err:
        log.exception("Unexpected error in get_dacs")
        raise HttpInternalError() from err


@router.get(
    "/dacs/{dac_id}",
    summary="Get a DAC",
    operation_id="getDac",
    response_model=DataAccessCommittee,
)
@TRACER.start_as_current_span("routes.get_dac")
async def get_dac(
    dac_id: str,
    registry: dummies.StudyRegistryDummy,
):
    """Get a data access committee by ID."""
    try:
        return await registry.get_dac(dac_id=dac_id)
    except StudyRegistryPort.DacNotFoundError as err:
        raise HttpDacNotFoundError(dac_id=dac_id) from err
    except Exception as err:
        log.exception("Unexpected error in get_dac")
        raise HttpInternalError() from err


@router.patch(
    "/dacs/{dac_id}",
    summary="Update a DAC",
    operation_id="updateDac",
    status_code=status.HTTP_204_NO_CONTENT,
)
@TRACER.start_as_current_span("routes.update_dac")
async def update_dac(
    dac_id: str,
    body: DacUpdateRequest,
    auth: AuthContextDep,
    registry: dummies.StudyRegistryDummy,
):
    """Update a data access committee."""
    require_steward(auth)
    try:
        await registry.update_dac(
            dac_id=dac_id,
            name=body.name,
            email=body.email,
            institute=body.institute,
            active=body.active,
        )
    except StudyRegistryPort.DacNotFoundError as err:
        raise HttpDacNotFoundError(dac_id=dac_id) from err
    except Exception as err:
        log.exception("Unexpected error in update_dac")
        raise HttpInternalError() from err


@router.delete(
    "/dacs/{dac_id}",
    summary="Delete a DAC",
    operation_id="deleteDac",
    status_code=status.HTTP_204_NO_CONTENT,
)
@TRACER.start_as_current_span("routes.delete_dac")
async def delete_dac(
    dac_id: str,
    auth: AuthContextDep,
    registry: dummies.StudyRegistryDummy,
):
    """Delete a data access committee."""
    require_steward(auth)
    try:
        await registry.delete_dac(dac_id=dac_id)
    except StudyRegistryPort.DacNotFoundError as err:
        raise HttpDacNotFoundError(dac_id=dac_id) from err
    except StudyRegistryPort.ReferenceConflictError as err:
        raise HttpReferenceConflictError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in delete_dac")
        raise HttpInternalError() from err


# ──────────────────── Data Access Policies ──────────────────────


@router.post(
    "/daps",
    summary="Create a DAP",
    operation_id="createDap",
    status_code=status.HTTP_201_CREATED,
)
@TRACER.start_as_current_span("routes.create_dap")
async def create_dap(
    body: DapCreateRequest,
    auth: AuthContextDep,
    registry: dummies.StudyRegistryDummy,
):
    """Create a new data access policy."""
    require_steward(auth)
    try:
        await registry.create_dap(
            id=body.id,
            name=body.name,
            description=body.description,
            text=body.text,
            url=body.url,
            duo_permission_id=body.duo_permission_id,
            duo_modifier_ids=body.duo_modifier_ids,
            dac_id=body.dac_id,
        )
    except StudyRegistryPort.DacNotFoundError as err:
        raise HttpDacNotFoundError(dac_id=body.dac_id) from err
    except StudyRegistryPort.DuplicateError as err:
        raise HttpDuplicateError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in create_dap")
        raise HttpInternalError() from err


@router.get(
    "/daps",
    summary="List DAPs",
    operation_id="getDaps",
    response_model=list[DataAccessPolicy],
)
@TRACER.start_as_current_span("routes.get_daps")
async def get_daps(
    registry: dummies.StudyRegistryDummy,
):
    """Get all data access policies."""
    try:
        return await registry.get_daps()
    except Exception as err:
        log.exception("Unexpected error in get_daps")
        raise HttpInternalError() from err


@router.get(
    "/daps/{dap_id}",
    summary="Get a DAP",
    operation_id="getDap",
    response_model=DataAccessPolicy,
)
@TRACER.start_as_current_span("routes.get_dap")
async def get_dap(
    dap_id: str,
    registry: dummies.StudyRegistryDummy,
):
    """Get a data access policy by ID."""
    try:
        return await registry.get_dap(dap_id=dap_id)
    except StudyRegistryPort.DapNotFoundError as err:
        raise HttpDapNotFoundError(dap_id=dap_id) from err
    except Exception as err:
        log.exception("Unexpected error in get_dap")
        raise HttpInternalError() from err


@router.patch(
    "/daps/{dap_id}",
    summary="Update a DAP",
    operation_id="updateDap",
    status_code=status.HTTP_204_NO_CONTENT,
)
@TRACER.start_as_current_span("routes.update_dap")
async def update_dap(
    dap_id: str,
    body: DapUpdateRequest,
    auth: AuthContextDep,
    registry: dummies.StudyRegistryDummy,
):
    """Update a data access policy."""
    require_steward(auth)
    try:
        await registry.update_dap(
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
    except StudyRegistryPort.DapNotFoundError as err:
        raise HttpDapNotFoundError(dap_id=dap_id) from err
    except StudyRegistryPort.DacNotFoundError as err:
        raise HttpDacNotFoundError(dac_id=body.dac_id or "") from err
    except Exception as err:
        log.exception("Unexpected error in update_dap")
        raise HttpInternalError() from err


@router.delete(
    "/daps/{dap_id}",
    summary="Delete a DAP",
    operation_id="deleteDap",
    status_code=status.HTTP_204_NO_CONTENT,
)
@TRACER.start_as_current_span("routes.delete_dap")
async def delete_dap(
    dap_id: str,
    auth: AuthContextDep,
    registry: dummies.StudyRegistryDummy,
):
    """Delete a data access policy."""
    require_steward(auth)
    try:
        await registry.delete_dap(dap_id=dap_id)
    except StudyRegistryPort.DapNotFoundError as err:
        raise HttpDapNotFoundError(dap_id=dap_id) from err
    except StudyRegistryPort.ReferenceConflictError as err:
        raise HttpReferenceConflictError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in delete_dap")
        raise HttpInternalError() from err


# ──────────────────────────── Datasets ──────────────────────────


@router.post(
    "/studies/{study_id}/datasets",
    summary="Create a dataset",
    operation_id="createDataset",
    status_code=status.HTTP_201_CREATED,
    response_model=Dataset,
)
@TRACER.start_as_current_span("routes.create_dataset")
async def create_dataset(
    study_id: str,
    body: DatasetCreateRequest,
    auth: AuthContextDep,
    registry: dummies.StudyRegistryDummy,
):
    """Create a dataset for a study."""
    require_steward(auth)
    try:
        return await registry.create_dataset(
            title=body.title,
            description=body.description,
            types=body.types,
            study_id=study_id,
            dap_id=body.dap_id,
            files=body.files,
        )
    except StudyRegistryPort.StudyNotFoundError as err:
        raise HttpStudyNotFoundError(study_id=study_id) from err
    except StudyRegistryPort.StatusConflictError as err:
        raise HttpStatusConflictError(detail=str(err)) from err
    except StudyRegistryPort.DapNotFoundError as err:
        raise HttpDapNotFoundError(dap_id=body.dap_id) from err
    except StudyRegistryPort.ValidationError as err:
        raise HttpValidationError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in create_dataset")
        raise HttpInternalError() from err


@router.get(
    "/datasets",
    summary="List datasets",
    operation_id="getDatasets",
    response_model=list[Dataset],
)
@TRACER.start_as_current_span("routes.get_datasets")
async def get_datasets(
    registry: dummies.StudyRegistryDummy,
    auth: OptionalAuthContextDep = None,
    dataset_type: str | None = Query(None),
    study_id: str | None = Query(None),
    text: str | None = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """Get a filtered list of datasets."""
    try:
        return await registry.get_datasets(
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


@router.get(
    "/datasets/{dataset_id}",
    summary="Get a dataset",
    operation_id="getDataset",
    response_model=Dataset,
)
@TRACER.start_as_current_span("routes.get_dataset")
async def get_dataset(
    dataset_id: str,
    registry: dummies.StudyRegistryDummy,
    auth: OptionalAuthContextDep = None,
):
    """Get a dataset by its accession ID."""
    try:
        return await registry.get_dataset(
            dataset_id=dataset_id,
            user_id=get_optional_user_id(auth),
            is_data_steward=is_optional_data_steward(auth),
        )
    except StudyRegistryPort.DatasetNotFoundError as err:
        raise HttpDatasetNotFoundError(dataset_id=dataset_id) from err
    except StudyRegistryPort.AccessDeniedError as err:
        raise HttpNotAuthorizedError() from err
    except Exception as err:
        log.exception("Unexpected error in get_dataset")
        raise HttpInternalError() from err


@router.patch(
    "/datasets/{dataset_id}",
    summary="Update a dataset",
    operation_id="updateDataset",
    status_code=status.HTTP_204_NO_CONTENT,
)
@TRACER.start_as_current_span("routes.update_dataset")
async def update_dataset(
    dataset_id: str,
    body: DatasetUpdateRequest,
    auth: AuthContextDep,
    registry: dummies.StudyRegistryDummy,
):
    """Update the DAP assignment for a dataset."""
    require_steward(auth)
    try:
        await registry.update_dataset(
            dataset_id=dataset_id, dap_id=body.dap_id
        )
    except StudyRegistryPort.DatasetNotFoundError as err:
        raise HttpDatasetNotFoundError(dataset_id=dataset_id) from err
    except StudyRegistryPort.DapNotFoundError as err:
        raise HttpDapNotFoundError(dap_id=body.dap_id) from err
    except Exception as err:
        log.exception("Unexpected error in update_dataset")
        raise HttpInternalError() from err


@router.delete(
    "/datasets/{dataset_id}",
    summary="Delete a dataset",
    operation_id="deleteDataset",
    status_code=status.HTTP_204_NO_CONTENT,
)
@TRACER.start_as_current_span("routes.delete_dataset")
async def delete_dataset(
    dataset_id: str,
    auth: AuthContextDep,
    registry: dummies.StudyRegistryDummy,
):
    """Delete a dataset."""
    require_steward(auth)
    try:
        await registry.delete_dataset(dataset_id=dataset_id)
    except StudyRegistryPort.DatasetNotFoundError as err:
        raise HttpDatasetNotFoundError(dataset_id=dataset_id) from err
    except StudyRegistryPort.StatusConflictError as err:
        raise HttpStatusConflictError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in delete_dataset")
        raise HttpInternalError() from err


# ──────────────────────── Resource Types ────────────────────────


@router.post(
    "/resource-types",
    summary="Create a resource type",
    operation_id="createResourceType",
    status_code=status.HTTP_201_CREATED,
    response_model=ResourceType,
)
@TRACER.start_as_current_span("routes.create_resource_type")
async def create_resource_type(
    body: ResourceTypeCreateRequest,
    auth: AuthContextDep,
    registry: dummies.StudyRegistryDummy,
):
    """Create a new resource type."""
    require_steward(auth)
    try:
        return await registry.create_resource_type(
            code=body.code,
            resource=TypedResource(body.resource),
            name=body.name,
            description=body.description,
        )
    except Exception as err:
        log.exception("Unexpected error in create_resource_type")
        raise HttpInternalError() from err


@router.get(
    "/resource-types",
    summary="List resource types",
    operation_id="getResourceTypes",
    response_model=list[ResourceType],
)
@TRACER.start_as_current_span("routes.get_resource_types")
async def get_resource_types(
    registry: dummies.StudyRegistryDummy,
    resource: str | None = Query(None),
    text: str | None = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """Get a filtered list of resource types."""
    try:
        typed_resource = TypedResource(resource) if resource else None
        return await registry.get_resource_types(
            resource=typed_resource, text=text, skip=skip, limit=limit
        )
    except Exception as err:
        log.exception("Unexpected error in get_resource_types")
        raise HttpInternalError() from err


@router.get(
    "/resource-types/{resource_type_id}",
    summary="Get a resource type",
    operation_id="getResourceType",
    response_model=ResourceType,
)
@TRACER.start_as_current_span("routes.get_resource_type")
async def get_resource_type(
    resource_type_id: UUID,
    registry: dummies.StudyRegistryDummy,
):
    """Get a resource type by ID."""
    try:
        return await registry.get_resource_type(
            resource_type_id=resource_type_id
        )
    except StudyRegistryPort.ResourceTypeNotFoundError as err:
        raise HttpResourceTypeNotFoundError() from err
    except Exception as err:
        log.exception("Unexpected error in get_resource_type")
        raise HttpInternalError() from err


@router.patch(
    "/resource-types/{resource_type_id}",
    summary="Update a resource type",
    operation_id="updateResourceType",
    status_code=status.HTTP_204_NO_CONTENT,
)
@TRACER.start_as_current_span("routes.update_resource_type")
async def update_resource_type(
    resource_type_id: UUID,
    body: ResourceTypeUpdateRequest,
    auth: AuthContextDep,
    registry: dummies.StudyRegistryDummy,
):
    """Update a resource type."""
    require_steward(auth)
    try:
        await registry.update_resource_type(
            resource_type_id=resource_type_id,
            name=body.name,
            description=body.description,
            active=body.active,
        )
    except StudyRegistryPort.ResourceTypeNotFoundError as err:
        raise HttpResourceTypeNotFoundError() from err
    except Exception as err:
        log.exception("Unexpected error in update_resource_type")
        raise HttpInternalError() from err


@router.delete(
    "/resource-types/{resource_type_id}",
    summary="Delete a resource type",
    operation_id="deleteResourceType",
    status_code=status.HTTP_204_NO_CONTENT,
)
@TRACER.start_as_current_span("routes.delete_resource_type")
async def delete_resource_type(
    resource_type_id: UUID,
    auth: AuthContextDep,
    registry: dummies.StudyRegistryDummy,
):
    """Delete a resource type."""
    require_steward(auth)
    try:
        await registry.delete_resource_type(
            resource_type_id=resource_type_id
        )
    except StudyRegistryPort.ResourceTypeNotFoundError as err:
        raise HttpResourceTypeNotFoundError() from err
    except StudyRegistryPort.ReferenceConflictError as err:
        raise HttpReferenceConflictError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in delete_resource_type")
        raise HttpInternalError() from err


# ──────────────────────── Accessions ────────────────────────────


@router.get(
    "/accessions/{accession_id}",
    summary="Get a primary accession",
    operation_id="getAccession",
    response_model=Accession,
)
@TRACER.start_as_current_span("routes.get_accession")
async def get_accession(
    accession_id: str,
    registry: dummies.StudyRegistryDummy,
):
    """Look up a primary accession."""
    try:
        return await registry.get_accession(accession_id=accession_id)
    except StudyRegistryPort.AccessionNotFoundError as err:
        raise HttpAccessionNotFoundError(accession_id=accession_id) from err
    except Exception as err:
        log.exception("Unexpected error in get_accession")
        raise HttpInternalError() from err


@router.get(
    "/accessions/{accession_id}/alt/{alt_type}",
    summary="Get an alternative accession",
    operation_id="getAltAccession",
    response_model=AltAccession,
)
@TRACER.start_as_current_span("routes.get_alt_accession")
async def get_alt_accession(
    accession_id: str,
    alt_type: AltAccessionType,
    registry: dummies.StudyRegistryDummy,
):
    """Look up an alternative accession (e.g. EGA, FILE_ID)."""
    try:
        return await registry.get_alt_accession(
            accession_id=accession_id, alt_type=alt_type
        )
    except StudyRegistryPort.AccessionNotFoundError as err:
        raise HttpAccessionNotFoundError(accession_id=accession_id) from err
    except Exception as err:
        log.exception("Unexpected error in get_alt_accession")
        raise HttpInternalError() from err


# ──────────────────────── Filenames ─────────────────────────────


@router.get(
    "/filenames/{study_id}",
    summary="Get file accession-to-name mapping",
    operation_id="getFilenames",
    response_model=dict[str, dict[str, str]],
)
@TRACER.start_as_current_span("routes.get_filenames")
async def get_filenames(
    study_id: str,
    auth: AuthContextDep,
    registry: dummies.StudyRegistryDummy,
):
    """Get file accession to filename/alias mapping for a study."""
    require_steward(auth)
    try:
        return await registry.get_filenames(study_id=study_id)
    except StudyRegistryPort.StudyNotFoundError as err:
        raise HttpStudyNotFoundError(study_id=study_id) from err
    except StudyRegistryPort.MetadataNotFoundError as err:
        raise HttpMetadataNotFoundError(study_id=study_id) from err
    except Exception as err:
        log.exception("Unexpected error in get_filenames")
        raise HttpInternalError() from err


@router.post(
    "/filenames/{study_id}",
    summary="Post file ID mappings",
    operation_id="postFilenames",
    status_code=status.HTTP_204_NO_CONTENT,
)
@TRACER.start_as_current_span("routes.post_filenames")
async def post_filenames(
    study_id: str,
    body: FileIdMappingRequest,
    auth: AuthContextDep,
    registry: dummies.StudyRegistryDummy,
):
    """Store file accession to internal file ID mappings."""
    require_steward(auth)
    try:
        await registry.post_filenames(
            study_id=study_id, file_id_map=body.file_id_map
        )
    except StudyRegistryPort.StudyNotFoundError as err:
        raise HttpStudyNotFoundError(study_id=study_id) from err
    except StudyRegistryPort.ValidationError as err:
        raise HttpValidationError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in post_filenames")
        raise HttpInternalError() from err


# ──────────────────────── Publish ───────────────────────────────


@router.post(
    "/studies/{study_id}/publish",
    summary="Publish a study",
    operation_id="publishStudy",
    status_code=status.HTTP_202_ACCEPTED,
)
@TRACER.start_as_current_span("routes.publish_study")
async def publish_study(
    study_id: str,
    auth: AuthContextDep,
    registry: dummies.StudyRegistryDummy,
):
    """Validate and publish a study's annotated experimental metadata."""
    require_steward(auth)
    try:
        await registry.publish_study(study_id=study_id)
    except StudyRegistryPort.StudyNotFoundError as err:
        raise HttpStudyNotFoundError(study_id=study_id) from err
    except StudyRegistryPort.ValidationError as err:
        raise HttpValidationError(detail=str(err)) from err
    except Exception as err:
        log.exception("Unexpected error in publish_study")
        raise HttpInternalError() from err
