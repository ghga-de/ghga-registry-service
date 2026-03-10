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

from srs.ports.inbound.accession import AccessionPort
from srs.ports.inbound.data_access import DataAccessPort
from srs.ports.inbound.dataset import DatasetPort
from srs.ports.inbound.filename import FilenamePort
from srs.ports.inbound.metadata import MetadataPort
from srs.ports.inbound.publication import PublicationPort
from srs.ports.inbound.resource_type import ResourceTypePort
from srs.ports.inbound.study import StudyPort
from srs.ports.inbound.study_registry import StudyRegistryPort

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
        accession_controller: AccessionPort,
        study_controller: StudyPort,
        dataset_controller: DatasetPort,
        metadata_controller: MetadataPort,
        publication_controller: PublicationPort,
        filename_controller: FilenamePort,
        resource_type_controller: ResourceTypePort,
        data_access: DataAccessPort,
    ):
        self.accessions = accession_controller
        self.data_access = data_access
        self.studies = study_controller
        self.datasets = dataset_controller
        self.metadata = metadata_controller
        self.publications = publication_controller
        self.filenames = filename_controller
        self.resource_types = resource_type_controller
