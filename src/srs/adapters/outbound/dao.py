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

"""Outbound adapter: MongoDB DAO factory for all SRS collections."""

from hexkit.protocols.dao import DaoFactoryProtocol

from srs.constants import (
    ACCESSIONS_COLLECTION,
    ALT_ACCESSIONS_COLLECTION,
    DACS_COLLECTION,
    DAPS_COLLECTION,
    DATASETS_COLLECTION,
    EM_ACCESSION_MAPS_COLLECTION,
    EXPERIMENTAL_METADATA_COLLECTION,
    PUBLICATIONS_COLLECTION,
    RESOURCE_TYPES_COLLECTION,
    STUDIES_COLLECTION,
)
from srs.core.models import (
    Accession,
    AltAccession,
    DataAccessCommittee,
    DataAccessPolicy,
    Dataset,
    EmAccessionMap,
    ExperimentalMetadata,
    Publication,
    ResourceType,
    Study,
)
from srs.ports.outbound.dao import (
    AccessionDao,
    AltAccessionDao,
    DataAccessCommitteeDao,
    DataAccessPolicyDao,
    DatasetDao,
    EmAccessionMapDao,
    ExperimentalMetadataDao,
    PublicationDao,
    ResourceTypeDao,
    StudyDao,
)


async def get_study_dao(*, dao_factory: DaoFactoryProtocol) -> StudyDao:
    """Get a DAO for the studies collection."""
    return await dao_factory.get_dao(
        name=STUDIES_COLLECTION,
        dto_model=Study,
        id_field="id",
    )


async def get_metadata_dao(
    *, dao_factory: DaoFactoryProtocol
) -> ExperimentalMetadataDao:
    """Get a DAO for the experimental metadata collection."""
    return await dao_factory.get_dao(
        name=EXPERIMENTAL_METADATA_COLLECTION,
        dto_model=ExperimentalMetadata,
        id_field="id",
    )


async def get_publication_dao(
    *, dao_factory: DaoFactoryProtocol
) -> PublicationDao:
    """Get a DAO for the publications collection."""
    return await dao_factory.get_dao(
        name=PUBLICATIONS_COLLECTION,
        dto_model=Publication,
        id_field="id",
    )


async def get_dac_dao(
    *, dao_factory: DaoFactoryProtocol
) -> DataAccessCommitteeDao:
    """Get a DAO for the DACs collection."""
    return await dao_factory.get_dao(
        name=DACS_COLLECTION,
        dto_model=DataAccessCommittee,
        id_field="id",
    )


async def get_dap_dao(
    *, dao_factory: DaoFactoryProtocol
) -> DataAccessPolicyDao:
    """Get a DAO for the DAPs collection."""
    return await dao_factory.get_dao(
        name=DAPS_COLLECTION,
        dto_model=DataAccessPolicy,
        id_field="id",
    )


async def get_dataset_dao(
    *, dao_factory: DaoFactoryProtocol
) -> DatasetDao:
    """Get a DAO for the datasets collection."""
    return await dao_factory.get_dao(
        name=DATASETS_COLLECTION,
        dto_model=Dataset,
        id_field="id",
    )


async def get_resource_type_dao(
    *, dao_factory: DaoFactoryProtocol
) -> ResourceTypeDao:
    """Get a DAO for the resource types collection."""
    return await dao_factory.get_dao(
        name=RESOURCE_TYPES_COLLECTION,
        dto_model=ResourceType,
        id_field="id",
    )


async def get_accession_dao(
    *, dao_factory: DaoFactoryProtocol
) -> AccessionDao:
    """Get a DAO for the accessions collection."""
    return await dao_factory.get_dao(
        name=ACCESSIONS_COLLECTION,
        dto_model=Accession,
        id_field="id",
    )


async def get_alt_accession_dao(
    *, dao_factory: DaoFactoryProtocol
) -> AltAccessionDao:
    """Get a DAO for the alternative accessions collection."""
    return await dao_factory.get_dao(
        name=ALT_ACCESSIONS_COLLECTION,
        dto_model=AltAccession,
        id_field="id",
    )


async def get_em_accession_map_dao(
    *, dao_factory: DaoFactoryProtocol
) -> EmAccessionMapDao:
    """Get a DAO for the EM accession maps collection."""
    return await dao_factory.get_dao(
        name=EM_ACCESSION_MAPS_COLLECTION,
        dto_model=EmAccessionMap,
        id_field="id",
    )
