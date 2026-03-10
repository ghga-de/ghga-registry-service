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

from uuid import UUID

from pydantic import BaseModel, EmailStr, Field, field_validator


# --- Study models ---


class StudyCreateRequest(BaseModel):
    """Request body for creating a study."""

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

    status: str | None = Field(None, description="New status for the study.")
    users: list[UUID] | None = Field(None, description="Updated user list.")
    approved_by: UUID | None = Field(None, description="UUID of approver.")


# --- Publication models ---


class PublicationCreateRequest(BaseModel):
    """Request body for creating a publication."""

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

    id: str = Field(
        ...,
        description="Short uppercase code derived from the name "
        "(e.g. 'GHGA_DAC'). Only uppercase letters, digits, and "
        "underscores are allowed.",
    )
    name: str = Field(..., description="Name of the DAC.")
    email: EmailStr = Field(..., description="Contact email.")
    institute: str = Field(..., description="Institute name.")

    @field_validator("id")
    @classmethod
    def validate_dac_id(cls, v: str) -> str:
        """Ensure the DAC id is a short uppercase code."""
        import re

        if not re.fullmatch(r"[A-Z][A-Z0-9_]{1,30}", v):
            raise ValueError(
                "DAC id must be 2-31 characters, start with an uppercase "
                "letter, and contain only uppercase letters, digits, and "
                "underscores."
            )
        return v


class DacUpdateRequest(BaseModel):
    """Request body for updating a DAC."""

    name: str | None = Field(None, description="Updated name.")
    email: EmailStr | None = Field(None, description="Updated email.")
    institute: str | None = Field(None, description="Updated institute.")
    active: bool | None = Field(None, description="Active status.")


# --- DAP models ---


class DapCreateRequest(BaseModel):
    """Request body for creating a DAP."""

    id: str = Field(
        ...,
        description="Short uppercase code derived from the name "
        "(e.g. 'GHGA_DAP'). Only uppercase letters, digits, and "
        "underscores are allowed.",
    )
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

    @field_validator("id")
    @classmethod
    def validate_dap_id(cls, v: str) -> str:
        """Ensure the DAP id is a short uppercase code."""
        import re

        if not re.fullmatch(r"[A-Z][A-Z0-9_]{1,30}", v):
            raise ValueError(
                "DAP id must be 2-31 characters, start with an uppercase "
                "letter, and contain only uppercase letters, digits, and "
                "underscores."
            )
        return v


class DapUpdateRequest(BaseModel):
    """Request body for updating a DAP."""

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

    dap_id: str = Field(..., description="New DAP ID.")


# --- ResourceType models ---


class ResourceTypeCreateRequest(BaseModel):
    """Request body for creating a resource type."""

    code: str = Field(..., description="Unique code (uppercase).")
    resource: str = Field(
        ..., description="Resource category (DATASET or STUDY)."
    )
    name: str = Field(..., description="Human-readable name.")
    description: str | None = Field(None, description="Description.")


class ResourceTypeUpdateRequest(BaseModel):
    """Request body for updating a resource type."""

    name: str | None = None
    description: str | None = None
    active: bool | None = None


# --- Experimental Metadata models ---


class MetadataUpsertRequest(BaseModel):
    """Request body for upserting experimental metadata."""

    metadata: dict = Field(
        ..., description="Free-form experimental metadata."
    )


# --- Filename / file-ID mapping models ---


class FileIdMappingRequest(BaseModel):
    """Request body for posting file ID mappings."""

    file_id_map: dict[str, str] = Field(
        ..., description="Map of file accession PIDs to internal file IDs."
    )
