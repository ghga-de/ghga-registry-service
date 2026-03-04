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

"""Core implementation of the Study Registry Service (composition root)."""

import logging

from hexkit.protocols.dao import ResourceNotFoundError

from srs.core.models import (
    Accession,
    AltAccession,
    AltAccessionType,
)
from srs.ports.inbound.data_access import DataAccessPort
from srs.ports.inbound.dataset import DatasetPort
from srs.ports.inbound.filename import FilenamePort
from srs.ports.inbound.metadata import MetadataPort
from srs.ports.inbound.publication import PublicationPort
from srs.ports.inbound.resource_type import ResourceTypePort
from srs.ports.inbound.study import StudyPort
from srs.ports.inbound.study_registry import StudyRegistryPort
from srs.ports.outbound.dao import (
    AccessionDao,
    AltAccessionDao,
)

log = logging.getLogger(__name__)


class StudyRegistryController(StudyRegistryPort):
    """Composition root for the Study Registry Service.

    Delegates study CRUD/publish operations to StudyController,
    metadata to MetadataController, publications to PublicationController,
    filenames to FilenameController, datasets to DatasetController,
    resource types to ResourceTypeController, and DAC/DAP to
    DataAccessController.  Owns accession operations directly.
    """

    def __init__(
        self,
        *,
        study_controller: StudyPort,
        dataset_controller: DatasetPort,
        metadata_controller: MetadataPort,
        publication_controller: PublicationPort,
        filename_controller: FilenamePort,
        resource_type_controller: ResourceTypePort,
        data_access: DataAccessPort,
        accession_dao: AccessionDao,
        alt_accession_dao: AltAccessionDao,
    ):
        self._study_controller = study_controller
        self._dataset_controller = dataset_controller
        self._metadata_controller = metadata_controller
        self._publication_controller = publication_controller
        self._filename_controller = filename_controller
        self._resource_type_controller = resource_type_controller
        self._data_access = data_access
        self._accession_dao = accession_dao
        self._alt_accession_dao = alt_accession_dao

    # --- Composite sub-ports ---

    @property
    def data_access(self) -> DataAccessPort:
        """Return the data-access sub-controller."""
        return self._data_access

    @property
    def studies(self) -> StudyPort:
        """Return the study sub-controller."""
        return self._study_controller

    @property
    def datasets(self) -> DatasetPort:
        """Return the dataset sub-controller."""
        return self._dataset_controller

    @property
    def metadata(self) -> MetadataPort:
        """Return the metadata sub-controller."""
        return self._metadata_controller

    @property
    def publications(self) -> PublicationPort:
        """Return the publication sub-controller."""
        return self._publication_controller

    @property
    def filenames(self) -> FilenamePort:
        """Return the filename sub-controller."""
        return self._filename_controller

    @property
    def resource_types(self) -> ResourceTypePort:
        """Return the resource type sub-controller."""
        return self._resource_type_controller

    # --- Accession operations ---

    async def get_accession(self, *, accession_id: str) -> Accession:
        """Get a primary accession by ID."""
        try:
            return await self._accession_dao.get_by_id(accession_id)
        except ResourceNotFoundError as err:
            raise self.AccessionNotFoundError(
                accession_id=accession_id
            ) from err

    async def get_alt_accession(
        self,
        *,
        accession_id: str,
        alt_type: AltAccessionType,
    ) -> AltAccession:
        """Get an alternative accession by ID and type."""
        async for alt in self._alt_accession_dao.find_all(
            mapping={"id": accession_id, "type": alt_type.value}
        ):
            return alt
        raise self.AccessionNotFoundError(accession_id=accession_id)
