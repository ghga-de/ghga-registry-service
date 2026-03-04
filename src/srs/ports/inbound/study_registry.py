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

from abc import ABC, abstractmethod
from uuid import UUID

from srs.core.models import (
    Accession,
    AltAccession,
    AltAccessionType,
    Dataset,
    ExperimentalMetadata,
    Publication,
    ResourceType,
    Study,
    StudyStatus,
    TypedResource,
)
from srs.ports.inbound.data_access import DataAccessPort


class StudyRegistryPort(ABC):
    """Inbound port defining all operations of the Study Registry Service."""

    # --- Error classes ---

    class RegistryError(RuntimeError):
        """Base error for all registry errors."""

    class StudyNotFoundError(RegistryError):
        """Raised when a study is not found."""

        def __init__(self, *, study_id: str):
            super().__init__(f"Study with ID {study_id} not found.")

    class PublicationNotFoundError(RegistryError):
        """Raised when a publication is not found."""

        def __init__(self, *, publication_id: str):
            super().__init__(
                f"Publication with ID {publication_id} not found."
            )

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
            super().__init__(
                f"ResourceType with ID {resource_type_id} not found."
            )

    class MetadataNotFoundError(RegistryError):
        """Raised when experimental metadata is not found."""

        def __init__(self, *, study_id: str):
            super().__init__(
                f"ExperimentalMetadata for study {study_id} not found."
            )

    class AccessionNotFoundError(RegistryError):
        """Raised when an accession is not found."""

        def __init__(self, *, accession_id: str):
            super().__init__(
                f"Accession with ID {accession_id} not found."
            )

    class StatusConflictError(RegistryError):
        """Raised when a status transition is not allowed."""

        def __init__(self, *, detail: str):
            super().__init__(detail)

    class ValidationError(RegistryError):
        """Raised when validation of a study fails."""

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

    # --- Data access (composite) ---

    @property
    @abstractmethod
    def data_access(self) -> DataAccessPort:
        """Return the data access controller for DAC/DAP operations."""
        ...

    # --- Study operations ---

    @abstractmethod
    async def create_study(
        self,
        *,
        title: str,
        description: str,
        types: list[str],
        affiliations: list[str],
        created_by: UUID,
    ) -> Study:
        """Create a new study with status PENDING.

        Returns the created Study with generated PID.
        """
        ...

    @abstractmethod
    async def get_studies(
        self,
        *,
        status: StudyStatus | None = None,
        study_type: str | None = None,
        text: str | None = None,
        skip: int = 0,
        limit: int = 100,
        user_id: UUID | None = None,
        is_data_steward: bool = False,
    ) -> list[Study]:
        """Get studies filtered by optional parameters.

        Only returns studies accessible to the user.
        """
        ...

    @abstractmethod
    async def get_study(
        self,
        *,
        study_id: str,
        user_id: UUID | None = None,
        is_data_steward: bool = False,
    ) -> Study:
        """Get a study by its PID.

        Raises:
        - StudyNotFoundError if the study does not exist.
        - AccessDeniedError if the user does not have access.
        """
        ...

    @abstractmethod
    async def update_study(
        self,
        *,
        study_id: str,
        status: StudyStatus | None = None,
        users: list[UUID] | None = None,
        approved_by: UUID | None = None,
    ) -> None:
        """Update study status and/or users.

        Raises:
        - StudyNotFoundError if the study does not exist.
        - StatusConflictError if the status transition is not allowed.
        - ValidationError if validation fails during status change.
        """
        ...

    @abstractmethod
    async def delete_study(self, *, study_id: str) -> None:
        """Delete a study and all related entities.

        Raises:
        - StudyNotFoundError if the study does not exist.
        - StatusConflictError if the study is not in PENDING status.
        """
        ...

    # --- ExperimentalMetadata operations ---

    @abstractmethod
    async def upsert_metadata(
        self, *, study_id: str, metadata: dict
    ) -> None:
        """Create or update experimental metadata for a study.

        Raises:
        - StudyNotFoundError if the study does not exist.
        - StatusConflictError if the study is not in PENDING status.
        """
        ...

    @abstractmethod
    async def get_metadata(self, *, study_id: str) -> ExperimentalMetadata:
        """Get experimental metadata for a study.

        Raises:
        - MetadataNotFoundError if metadata is not found.
        """
        ...

    @abstractmethod
    async def delete_metadata(self, *, study_id: str) -> None:
        """Delete experimental metadata for a study.

        Raises:
        - StudyNotFoundError if the study does not exist.
        - StatusConflictError if the study is not in PENDING status.
        """
        ...

    # --- Publication operations ---

    @abstractmethod
    async def create_publication(
        self,
        *,
        title: str,
        abstract: str | None,
        authors: list[str],
        year: int,
        journal: str | None,
        doi: str | None,
        study_id: str,
    ) -> Publication:
        """Create or update a publication for a study.

        Raises:
        - StudyNotFoundError if the study does not exist.
        - StatusConflictError if the study is not in PENDING status.
        """
        ...

    @abstractmethod
    async def get_publications(
        self,
        *,
        year: int | None = None,
        text: str | None = None,
        skip: int = 0,
        limit: int = 100,
        user_id: UUID | None = None,
        is_data_steward: bool = False,
    ) -> list[Publication]:
        """Get publications filtered by optional parameters."""
        ...

    @abstractmethod
    async def get_publication(
        self,
        *,
        publication_id: str,
        user_id: UUID | None = None,
        is_data_steward: bool = False,
    ) -> Publication:
        """Get a publication by its PID.

        Raises:
        - PublicationNotFoundError if the publication does not exist.
        - AccessDeniedError if the user does not have access.
        """
        ...

    @abstractmethod
    async def delete_publication(self, *, publication_id: str) -> None:
        """Delete a publication and its accession.

        Raises:
        - PublicationNotFoundError if the publication does not exist.
        - StatusConflictError if the study is not in PENDING status.
        """
        ...

    # --- Dataset operations ---

    @abstractmethod
    async def create_dataset(
        self,
        *,
        title: str,
        description: str,
        types: list[str],
        study_id: str,
        dap_id: str,
        files: list[str],
    ) -> Dataset:
        """Create or update a dataset for a study.

        Raises:
        - StudyNotFoundError if the study does not exist.
        - StatusConflictError if the study is not in PENDING status.
        - DapNotFoundError if the DAP does not exist.
        - ValidationError if files don't exist in EM or are duplicated.
        """
        ...

    @abstractmethod
    async def get_datasets(
        self,
        *,
        dataset_type: str | None = None,
        study_id: str | None = None,
        text: str | None = None,
        skip: int = 0,
        limit: int = 100,
        user_id: UUID | None = None,
        is_data_steward: bool = False,
    ) -> list[Dataset]:
        """Get datasets filtered by optional parameters."""
        ...

    @abstractmethod
    async def get_dataset(
        self,
        *,
        dataset_id: str,
        user_id: UUID | None = None,
        is_data_steward: bool = False,
    ) -> Dataset:
        """Get a dataset by its PID.

        Raises:
        - DatasetNotFoundError if the dataset does not exist.
        - AccessDeniedError if the user does not have access.
        """
        ...

    @abstractmethod
    async def update_dataset(
        self, *, dataset_id: str, dap_id: str
    ) -> None:
        """Update the DAP assignment for a dataset.

        Raises:
        - DatasetNotFoundError if the dataset does not exist.
        - DapNotFoundError if the new DAP does not exist.
        """
        ...

    @abstractmethod
    async def delete_dataset(self, *, dataset_id: str) -> None:
        """Delete a dataset and its accession.

        Raises:
        - DatasetNotFoundError if the dataset does not exist.
        - StatusConflictError if the study is not in PENDING status.
        """
        ...

    # --- ResourceType operations ---

    @abstractmethod
    async def create_resource_type(
        self,
        *,
        code: str,
        resource: TypedResource,
        name: str,
        description: str | None,
    ) -> ResourceType:
        """Create a new resource type.

        Returns the created ResourceType.
        """
        ...

    @abstractmethod
    async def get_resource_types(
        self,
        *,
        resource: TypedResource | None = None,
        text: str | None = None,
        skip: int = 0,
        limit: int = 100,
    ) -> list[ResourceType]:
        """Get resource types filtered by optional parameters."""
        ...

    @abstractmethod
    async def get_resource_type(
        self, *, resource_type_id: UUID
    ) -> ResourceType:
        """Get a resource type by internal ID.

        Raises:
        - ResourceTypeNotFoundError if the resource type does not exist.
        """
        ...

    @abstractmethod
    async def update_resource_type(
        self,
        *,
        resource_type_id: UUID,
        name: str | None = None,
        description: str | None = None,
        active: bool | None = None,
    ) -> None:
        """Update a resource type.

        Raises:
        - ResourceTypeNotFoundError if the resource type does not exist.
        """
        ...

    @abstractmethod
    async def delete_resource_type(
        self, *, resource_type_id: UUID
    ) -> None:
        """Delete a resource type.

        Raises:
        - ResourceTypeNotFoundError if the resource type does not exist.
        - ReferenceConflictError if the type is still referenced.
        """
        ...

    # --- Accession operations ---

    @abstractmethod
    async def get_accession(self, *, accession_id: str) -> Accession:
        """Get a primary accession by ID.

        Raises:
        - AccessionNotFoundError if the accession does not exist.
        """
        ...

    @abstractmethod
    async def get_alt_accession(
        self, *, accession_id: str, alt_type: AltAccessionType
    ) -> AltAccession:
        """Get an alternative accession by ID and type.

        Raises:
        - AccessionNotFoundError if the alt accession does not exist.
        """
        ...

    # --- Filename operations ---

    @abstractmethod
    async def get_filenames(
        self, *, study_id: str
    ) -> dict[str, dict[str, str]]:
        """Get file accession to filename/alias mapping for a study.

        Returns a map from file accessions to {name, alias} objects.

        Raises:
        - StudyNotFoundError if the study does not exist.
        """
        ...

    @abstractmethod
    async def post_filenames(
        self, *, study_id: str, file_id_map: dict[str, str]
    ) -> None:
        """Store file accession to internal file ID mappings.

        Creates AltAccession entries with type FILE_ID and republishes
        the mapping as an event.

        Raises:
        - StudyNotFoundError if the study does not exist.
        - ValidationError if accessions don't belong to the study.
        """
        ...

    # --- Publish operations ---

    @abstractmethod
    async def publish_study(self, *, study_id: str) -> None:
        """Validate and publish a study.

        Creates accession numbers for EM resources and publishes
        an AnnotatedExperimentalMetadata event.

        Raises:
        - StudyNotFoundError if the study does not exist.
        - ValidationError if the study is not complete or valid.
        """
        ...
