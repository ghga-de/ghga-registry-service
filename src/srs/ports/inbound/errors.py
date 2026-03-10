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

"""Shared error hierarchy for all inbound ports.

Every port re-exports the subset it needs as class-level attributes so that
callers can write ``StudyPort.StudyNotFoundError`` or
``StudyRegistryPort.StudyNotFoundError`` interchangeably – both resolve to the
**same** class.
"""

from uuid import UUID


class RegistryError(RuntimeError):
    """Base error for all registry operations."""


class StudyNotFoundError(RegistryError):
    """Raised when a study is not found."""

    def __init__(self, *, study_id: str):
        super().__init__(f"Study with ID {study_id} not found.")


class PublicationNotFoundError(RegistryError):
    """Raised when a publication is not found."""

    def __init__(self, *, publication_id: str):
        super().__init__(f"Publication with ID {publication_id} not found.")


class DatasetNotFoundError(RegistryError):
    """Raised when a dataset is not found."""

    def __init__(self, *, dataset_id: str):
        super().__init__(f"Dataset with ID {dataset_id} not found.")


class DacNotFoundError(RegistryError):
    """Raised when a DAC is not found."""

    def __init__(self, *, dac_id: str):
        super().__init__(f"DAC with ID {dac_id} not found.")


class DapNotFoundError(RegistryError):
    """Raised when a DAP is not found."""

    def __init__(self, *, dap_id: str):
        super().__init__(f"DAP with ID {dap_id} not found.")


class ResourceTypeNotFoundError(RegistryError):
    """Raised when a resource type is not found."""

    def __init__(self, *, resource_type_id: UUID):
        super().__init__(f"ResourceType with ID {resource_type_id} not found.")


class MetadataNotFoundError(RegistryError):
    """Raised when experimental metadata is not found."""

    def __init__(self, *, study_id: str):
        super().__init__(f"ExperimentalMetadata for study {study_id} not found.")


class AccessionNotFoundError(RegistryError):
    """Raised when an accession is not found."""

    def __init__(self, *, accession_id: str):
        super().__init__(f"Accession with ID {accession_id} not found.")


class StatusConflictError(RegistryError):
    """Raised when a status transition is not allowed."""

    def __init__(self, *, detail: str):
        super().__init__(detail)


class ValidationError(RegistryError):
    """Raised when validation fails."""

    def __init__(self, *, detail: str):
        super().__init__(detail)


class ReferenceConflictError(RegistryError):
    """Raised when deleting an entity that is still referenced."""

    def __init__(self, *, detail: str):
        super().__init__(detail)


class DuplicateError(RegistryError):
    """Raised when an entity with the same ID already exists."""

    def __init__(self, *, detail: str):
        super().__init__(detail)


class AccessDeniedError(RegistryError):
    """Raised when the user does not have access."""

    def __init__(self, *, detail: str = "Access denied."):
        super().__init__(detail)
