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

"""REST request/response models for the Study Registry Service."""

from datetime import date
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field


# --- Study models ---


class StudyCreateRequest(BaseModel):
    """Request body for creating a study."""

    model_config = ConfigDict(title="StudyCreate")

    title: str = Field(..., description="Title of the study.")
    description: str = Field(..., description="Description of the study.")
    types: list[str] = Field(
        default_factory=list, description="Type codes for the study."
    )
    affiliations: list[str] = Field(
        default_factory=list, description="Affiliations of the study."
    )


class StudyUpdateRequest(BaseModel):
    """Request body for updating a study."""

    model_config = ConfigDict(title="StudyUpdate")

    status: str | None = Field(None, description="New status for the study.")
    users: list[UUID] | None = Field(None, description="Updated user list.")
    approved_by: UUID | None = Field(None, description="UUID of approver.")


# --- Publication models ---


class PublicationCreateRequest(BaseModel):
    """Request body for creating a publication."""

    model_config = ConfigDict(title="PublicationCreate")

    title: str = Field(..., description="Title of the publication.")
    abstract: str | None = Field(None, description="Abstract text.")
    authors: list[str] = Field(
        default_factory=list, description="List of authors."
    )
    year: int = Field(..., description="Publication year.")
    journal: str | None = Field(None, description="Journal name.")
    doi: str | None = Field(None, description="DOI of the publication.")


# --- DAC models ---


class DacCreateRequest(BaseModel):
    """Request body for creating a DAC."""

    model_config = ConfigDict(title="DacCreate")

    id: str = Field(..., description="Unique DAC identifier.")
    name: str = Field(..., description="Name of the DAC.")
    email: EmailStr = Field(..., description="Contact email.")
    institute: str = Field(..., description="Institute name.")


class DacUpdateRequest(BaseModel):
    """Request body for updating a DAC."""

    model_config = ConfigDict(title="DacUpdate")

    name: str | None = Field(None, description="Updated name.")
    email: EmailStr | None = Field(None, description="Updated email.")
    institute: str | None = Field(None, description="Updated institute.")
    active: bool | None = Field(None, description="Active status.")


# --- DAP models ---


class DapCreateRequest(BaseModel):
    """Request body for creating a DAP."""

    model_config = ConfigDict(title="DapCreate")

    id: str = Field(..., description="Unique DAP identifier.")
    name: str = Field(..., description="Name of the DAP.")
    description: str = Field(..., description="Description.")
    text: str = Field(..., description="Full policy text.")
    url: str | None = Field(None, description="URL to the policy.")
    duo_permission_id: str = Field(
        ..., description="DUO data use permission code."
    )
    duo_modifier_ids: list[str] = Field(
        default_factory=list, description="DUO modifier codes."
    )
    dac_id: str = Field(..., description="Associated DAC ID.")


class DapUpdateRequest(BaseModel):
    """Request body for updating a DAP."""

    model_config = ConfigDict(title="DapUpdate")

    name: str | None = None
    description: str | None = None
    text: str | None = None
    url: str | None = None
    duo_permission_id: str | None = None
    duo_modifier_ids: list[str] | None = None
    dac_id: str | None = None
    active: bool | None = None


# --- Dataset models ---


class DatasetCreateRequest(BaseModel):
    """Request body for creating a dataset."""

    model_config = ConfigDict(title="DatasetCreate")

    title: str = Field(..., description="Title of the dataset.")
    description: str = Field(..., description="Description.")
    types: list[str] = Field(
        default_factory=list, description="Type codes."
    )
    dap_id: str = Field(..., description="Associated DAP ID.")
    files: list[str] = Field(
        default_factory=list,
        description="File aliases from experimental metadata.",
    )


class DatasetUpdateRequest(BaseModel):
    """Request body for updating a dataset."""

    model_config = ConfigDict(title="DatasetUpdate")

    dap_id: str = Field(..., description="New DAP ID.")


# --- ResourceType models ---


class ResourceTypeCreateRequest(BaseModel):
    """Request body for creating a resource type."""

    model_config = ConfigDict(title="ResourceTypeCreate")

    code: str = Field(..., description="Unique code (uppercase).")
    resource: str = Field(
        ..., description="Resource category (DATASET or STUDY)."
    )
    name: str = Field(..., description="Human-readable name.")
    description: str | None = Field(None, description="Description.")


class ResourceTypeUpdateRequest(BaseModel):
    """Request body for updating a resource type."""

    model_config = ConfigDict(title="ResourceTypeUpdate")

    name: str | None = None
    description: str | None = None
    active: bool | None = None


# --- Experimental Metadata models ---


class MetadataUpsertRequest(BaseModel):
    """Request body for upserting experimental metadata."""

    model_config = ConfigDict(title="MetadataUpsert")

    metadata: dict = Field(
        ..., description="Free-form experimental metadata."
    )


# --- Filename / file-ID mapping models ---


class FileIdMappingRequest(BaseModel):
    """Request body for posting file ID mappings."""

    model_config = ConfigDict(title="FileIdMapping")

    file_id_map: dict[str, str] = Field(
        ..., description="Map of file accession PIDs to internal file IDs."
    )
