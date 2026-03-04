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

    @property
    @abstractmethod
    def data_access(self) -> DataAccessPort:
        """Return the data access controller for DAC/DAP operations."""
        ...

    @property
    @abstractmethod
    def studies(self) -> StudyPort:
        """Return the study controller for study CRUD and publish operations."""
        ...

    @property
    @abstractmethod
    def datasets(self) -> DatasetPort:
        """Return the dataset controller for dataset operations."""
        ...

    @property
    @abstractmethod
    def metadata(self) -> MetadataPort:
        """Return the metadata controller for experimental metadata operations."""
        ...

    @property
    @abstractmethod
    def publications(self) -> PublicationPort:
        """Return the publication controller for publication operations."""
        ...

    @property
    @abstractmethod
    def filenames(self) -> FilenamePort:
        """Return the filename controller for filename operations."""
        ...

    @property
    @abstractmethod
    def resource_types(self) -> ResourceTypePort:
        """Return the resource type controller for resource type operations."""
        ...

    # --- Delegated Study operations ---

    async def create_study(
        self,
        *,
        title: str,
        description: str,
        types: list[str],
        affiliations: list[str],
        created_by: UUID,
    ) -> Study:
        """Create a new study with status PENDING."""
        return await self.studies.create_study(
            title=title,
            description=description,
            types=types,
            affiliations=affiliations,
            created_by=created_by,
        )

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
        """Get studies filtered by optional parameters."""
        return await self.studies.get_studies(
            status=status,
            study_type=study_type,
            text=text,
            skip=skip,
            limit=limit,
            user_id=user_id,
            is_data_steward=is_data_steward,
        )

    async def get_study(
        self,
        *,
        study_id: str,
        user_id: UUID | None = None,
        is_data_steward: bool = False,
    ) -> Study:
        """Get a study by its PID."""
        return await self.studies.get_study(
            study_id=study_id,
            user_id=user_id,
            is_data_steward=is_data_steward,
        )

    async def update_study(
        self,
        *,
        study_id: str,
        status: StudyStatus | None = None,
        users: list[UUID] | None = None,
        approved_by: UUID | None = None,
    ) -> None:
        """Update study status and/or users."""
        await self.studies.update_study(
            study_id=study_id,
            status=status,
            users=users,
            approved_by=approved_by,
        )

    async def delete_study(self, *, study_id: str) -> None:
        """Delete a study and all related entities."""
        await self.studies.delete_study(study_id=study_id)

    # --- Delegated Metadata operations ---

    async def upsert_metadata(
        self, *, study_id: str, metadata: dict
    ) -> None:
        """Create or update experimental metadata for a study."""
        await self.metadata.upsert_metadata(study_id=study_id, metadata=metadata)

    async def get_metadata(self, *, study_id: str) -> ExperimentalMetadata:
        """Get experimental metadata for a study."""
        return await self.metadata.get_metadata(study_id=study_id)

    async def delete_metadata(self, *, study_id: str) -> None:
        """Delete experimental metadata for a study."""
        await self.metadata.delete_metadata(study_id=study_id)

    # --- Delegated Publication operations ---

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
        """Create or update a publication for a study."""
        return await self.publications.create_publication(
            title=title,
            abstract=abstract,
            authors=authors,
            year=year,
            journal=journal,
            doi=doi,
            study_id=study_id,
        )

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
        return await self.publications.get_publications(
            year=year,
            text=text,
            skip=skip,
            limit=limit,
            user_id=user_id,
            is_data_steward=is_data_steward,
        )

    async def get_publication(
        self,
        *,
        publication_id: str,
        user_id: UUID | None = None,
        is_data_steward: bool = False,
    ) -> Publication:
        """Get a publication by its PID."""
        return await self.publications.get_publication(
            publication_id=publication_id,
            user_id=user_id,
            is_data_steward=is_data_steward,
        )

    async def delete_publication(self, *, publication_id: str) -> None:
        """Delete a publication and its accession."""
        await self.publications.delete_publication(publication_id=publication_id)

    # --- Delegated Dataset operations ---

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
        """Create or update a dataset for a study."""
        return await self.datasets.create_dataset(
            title=title,
            description=description,
            types=types,
            study_id=study_id,
            dap_id=dap_id,
            files=files,
        )

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
        return await self.datasets.get_datasets(
            dataset_type=dataset_type,
            study_id=study_id,
            text=text,
            skip=skip,
            limit=limit,
            user_id=user_id,
            is_data_steward=is_data_steward,
        )

    async def get_dataset(
        self,
        *,
        dataset_id: str,
        user_id: UUID | None = None,
        is_data_steward: bool = False,
    ) -> Dataset:
        """Get a dataset by its PID."""
        return await self.datasets.get_dataset(
            dataset_id=dataset_id,
            user_id=user_id,
            is_data_steward=is_data_steward,
        )

    async def update_dataset(
        self, *, dataset_id: str, dap_id: str
    ) -> None:
        """Update the DAP assignment for a dataset."""
        await self.datasets.update_dataset(dataset_id=dataset_id, dap_id=dap_id)

    async def delete_dataset(self, *, dataset_id: str) -> None:
        """Delete a dataset and its accession."""
        await self.datasets.delete_dataset(dataset_id=dataset_id)

    # --- Delegated ResourceType operations ---

    async def create_resource_type(
        self,
        *,
        code: str,
        resource: TypedResource,
        name: str,
        description: str | None,
    ) -> ResourceType:
        """Create a new resource type."""
        return await self.resource_types.create_resource_type(
            code=code, resource=resource, name=name, description=description
        )

    async def get_resource_types(
        self,
        *,
        resource: TypedResource | None = None,
        text: str | None = None,
        skip: int = 0,
        limit: int = 100,
    ) -> list[ResourceType]:
        """Get resource types filtered by optional parameters."""
        return await self.resource_types.get_resource_types(
            resource=resource, text=text, skip=skip, limit=limit
        )

    async def get_resource_type(
        self, *, resource_type_id: UUID
    ) -> ResourceType:
        """Get a resource type by internal ID."""
        return await self.resource_types.get_resource_type(
            resource_type_id=resource_type_id
        )

    async def update_resource_type(
        self,
        *,
        resource_type_id: UUID,
        name: str | None = None,
        description: str | None = None,
        active: bool | None = None,
    ) -> None:
        """Update a resource type."""
        await self.resource_types.update_resource_type(
            resource_type_id=resource_type_id,
            name=name,
            description=description,
            active=active,
        )

    async def delete_resource_type(
        self, *, resource_type_id: UUID
    ) -> None:
        """Delete a resource type."""
        await self.resource_types.delete_resource_type(
            resource_type_id=resource_type_id
        )

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

    # --- Delegated Filename operations ---

    async def get_filenames(
        self, *, study_id: str
    ) -> dict[str, dict[str, str]]:
        """Get file accession to filename/alias mapping for a study."""
        return await self.filenames.get_filenames(study_id=study_id)

    async def post_filenames(
        self, *, study_id: str, file_id_map: dict[str, str]
    ) -> None:
        """Store file accession to internal file ID mappings."""
        await self.filenames.post_filenames(
            study_id=study_id, file_id_map=file_id_map
        )

    # --- Delegated Publish operations ---

    async def publish_study(self, *, study_id: str) -> None:
        """Validate and publish a study."""
        await self.studies.publish_study(study_id=study_id)
