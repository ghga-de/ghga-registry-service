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

"""Entity models and enumerations for the Study Registry Service."""

from datetime import date
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, EmailStr


# --- Enumerations ---


class StudyStatus(StrEnum):
    """All possible states of a Study."""

    PENDING = "PENDING"
    FROZEN = "FROZEN"
    APPROVED = "APPROVED"
    PERSISTED = "PERSISTED"


class TypedResource(StrEnum):
    """Resources whose types are managed by this service."""

    DATASET = "DATASET"
    STUDY = "STUDY"


class AccessionType(StrEnum):
    """Types of primary accessions."""

    # Experimental Metadata
    ANALYSIS = "ANALYSIS"
    ANALYSIS_METHOD = "ANALYSIS_METHOD"
    FILE = "FILE"
    EXPERIMENT = "EXPERIMENT"
    EXPERIMENT_METHOD = "EXPERIMENT_METHOD"
    INDIVIDUAL = "INDIVIDUAL"
    SAMPLE = "SAMPLE"
    # Administrative Metadata
    DATASET = "DATASET"
    PUBLICATION = "PUBLICATION"
    STUDY = "STUDY"


class AltAccessionType(StrEnum):
    """Kinds of alternative accessions."""

    EGA = "EGA"
    FILE_ID = "FILE_ID"
    GHGA_LEGACY = "GHGA_LEGACY"


class DuoPermission(StrEnum):
    """DUO data use permissions (descendants of DUO:0000001).

    See https://github.com/EBISPOT/DUO/blob/master/duo.csv
    """

    GRU = "DUO:0000042"
    HMB = "DUO:0000006"
    DS = "DUO:0000007"
    NRES = "DUO:0000004"
    NMDS = "DUO:0000015"


class DuoModifier(StrEnum):
    """DUO data use modifiers (descendants of DUO:0000017).

    See https://github.com/EBISPOT/DUO/blob/master/duo.csv
    """

    POA = "DUO:0000011"
    RS = "DUO:0000012"
    NRES = "DUO:0000018"
    PUB = "DUO:0000019"
    COL = "DUO:0000020"
    IRB = "DUO:0000021"
    GS = "DUO:0000022"
    CC = "DUO:0000023"
    RTN = "DUO:0000024"
    TS = "DUO:0000025"
    US = "DUO:0000026"
    PS = "DUO:0000027"
    MOR = "DUO:0000028"
    ISR = "DUO:0000029"
    FRR = "DUO:0000040"
    NAGR = "DUO:0000041"
    GSO = "DUO:0000043"
    NCTRL = "DUO:0000044"
    NPU = "DUO:0000045"
    NCU = "DUO:0000046"


# --- Entity Models ---


class Study(BaseModel):
    """The central container for datasets, publications, and EM."""

    id: str
    title: str
    description: str
    types: list[str]
    affiliations: list[str]
    status: StudyStatus = StudyStatus.PENDING
    users: list[UUID] | None = None
    created: date
    created_by: UUID
    approved_by: UUID | None = None


class ExperimentalMetadata(BaseModel):
    """Stores experimental metadata belonging to a study as a generic JSON object."""

    id: str
    metadata: dict
    submitted: date


class Publication(BaseModel):
    """Citation reference for a study. Immutable with a PID."""

    id: str
    title: str
    abstract: str | None = None
    authors: list[str]
    year: int
    journal: str | None = None
    doi: str | None = None
    study_id: str
    created: date


class DataAccessCommittee(BaseModel):
    """Describes a Data Access Committee (DAC). Mutable, managed independently."""

    id: str
    name: str
    email: EmailStr
    institute: str
    created: date
    changed: date
    active: bool = True


class DataAccessPolicy(BaseModel):
    """Describes a policy for data access (DAP). Mutable, belongs to one DAC."""

    id: str
    name: str
    description: str
    text: str
    url: str | None = None
    duo_permission_id: DuoPermission
    duo_modifier_ids: list[DuoModifier] = []
    dac_id: str
    created: date
    changed: date
    active: bool = True


class Dataset(BaseModel):
    """Describes a set of files; smallest unit for data access requests."""

    id: str
    title: str
    description: str
    types: list[str]
    study_id: str
    dap_id: str
    files: list[str]
    created: date
    changed: date


class ResourceType(BaseModel):
    """Holds the possible types for studies and datasets."""

    id: UUID
    code: str
    resource: TypedResource
    name: str
    description: str | None = None
    created: date
    changed: date
    active: bool = True


class Accession(BaseModel):
    """Stores all existing primary accessions."""

    id: str
    type: AccessionType
    created: date
    superseded_by: str | None = None


class AltAccession(BaseModel):
    """Stores alternative accessions referencing a primary accession."""

    id: str
    pid: str
    type: AltAccessionType
    created: date


class EmAccessionMap(BaseModel):
    """Stores mappings from submitted IDs to primary accessions for a study."""

    id: str
    maps: dict[str, dict[str, str]]


# --- Event payload models ---


class PublicationNested(BaseModel):
    """Publication info nested within a study for AEM events."""

    id: str
    title: str
    abstract: str | None = None
    authors: list[str]
    year: int
    journal: str | None = None
    doi: str | None = None


class StudyWithPublication(BaseModel):
    """Study with nested publication for AEM events."""

    id: str
    title: str
    description: str
    types: list[str]
    affiliations: list[str]
    status: StudyStatus
    publication: PublicationNested | None = None


class DataAccessPolicyNested(BaseModel):
    """DAP with nested DAC for AEM dataset events."""

    id: str
    name: str
    description: str
    text: str
    url: str | None = None
    duo_permission_id: DuoPermission
    duo_modifier_ids: list[DuoModifier] = []
    dac: DataAccessCommittee


class DatasetWithDap(BaseModel):
    """Dataset with nested DAP and DAC for AEM events."""

    id: str
    title: str
    description: str
    types: list[str]
    files: list[str]
    dap: DataAccessPolicyNested


class AnnotatedExperimentalMetadata(BaseModel):
    """Annotated Experimental Metadata (AEM) published as an event."""

    metadata: dict
    accessions: dict[str, dict[str, str]]
    study: StudyWithPublication
    datasets: list[DatasetWithDap]
