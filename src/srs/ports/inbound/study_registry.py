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

"""Defines the main Study Registry Service inbound port."""

from abc import ABC

from srs.ports.inbound.accession import AccessionPort
from srs.ports.inbound.data_access import DataAccessPort
from srs.ports.inbound.dataset import DatasetPort
from srs.ports.inbound.errors import (
    AccessDeniedError,
    AccessionNotFoundError,
    DacNotFoundError,
    DapNotFoundError,
    DatasetNotFoundError,
    DuplicateError,
    MetadataNotFoundError,
    PublicationNotFoundError,
    ReferenceConflictError,
    RegistryError,
    ResourceTypeNotFoundError,
    StatusConflictError,
    StudyNotFoundError,
    ValidationError,
)
from srs.ports.inbound.filename import FilenamePort
from srs.ports.inbound.metadata import MetadataPort
from srs.ports.inbound.publication import PublicationPort
from srs.ports.inbound.resource_type import ResourceTypePort
from srs.ports.inbound.study import StudyPort


class StudyRegistryPort(ABC):
    """Inbound port defining all operations of the Study Registry Service."""

    # --- Error classes (shared with sub-ports via errors module) ---

    RegistryError = RegistryError
    StudyNotFoundError = StudyNotFoundError
    PublicationNotFoundError = PublicationNotFoundError
    DatasetNotFoundError = DatasetNotFoundError
    DacNotFoundError = DacNotFoundError
    DapNotFoundError = DapNotFoundError
    ResourceTypeNotFoundError = ResourceTypeNotFoundError
    MetadataNotFoundError = MetadataNotFoundError
    AccessionNotFoundError = AccessionNotFoundError
    StatusConflictError = StatusConflictError
    ValidationError = ValidationError
    ReferenceConflictError = ReferenceConflictError
    DuplicateError = DuplicateError
    AccessDeniedError = AccessDeniedError

    # --- Composite sub-ports ---

    accessions: AccessionPort
    data_access: DataAccessPort
    studies: StudyPort
    datasets: DatasetPort
    metadata: MetadataPort
    publications: PublicationPort
    filenames: FilenamePort
    resource_types: ResourceTypePort
