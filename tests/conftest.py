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

"""Shared pytest fixtures for the SRS test suite."""

from collections.abc import AsyncIterator, Mapping
from typing import Any
from uuid import UUID

import pytest
from ghga_service_commons.utils.jwt_helpers import generate_jwk
from hexkit.providers.testing.dao import BaseInMemDao, new_mock_dao_class
from hexkit.providers.testing.eventpub import InMemEventPublisher, InMemEventStore
from pydantic import BaseModel

from srs.adapters.outbound.event_pub import EventPubConfig, EventPubTranslator
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
from srs.core.data_access import DataAccessController
from srs.core.dataset import DatasetController
from srs.core.filename import FilenameController
from srs.core.metadata import MetadataController
from srs.core.publication import PublicationController
from srs.core.resource_type import ResourceTypeController
from srs.core.study import StudyController
from srs.core.accession import AccessionController
from srs.core.study_registry import StudyRegistryController
from tests.fixtures import ConfigFixture
from tests.fixtures.config import get_config
from tests.fixtures.joint import JointFixture, joint_fixture  # noqa: F401

# Re-export hexkit container/fixture definitions so pytest can discover them
from hexkit.providers.akafka.testutils import (  # noqa: F401
    kafka_container_fixture,
    kafka_fixture,
)
from hexkit.providers.mongodb.testutils import (  # noqa: F401
    mongodb_container_fixture,
    mongodb_fixture,
)


def _safe_mock_dao_class(
    *, dto_model: type[BaseModel], id_field: str
) -> type[BaseInMemDao]:
    """Create a mock DAO class with two test-safety patches:

    1. ``find_all`` snapshots ``self.resources`` before iterating so that
       callers can delete entries during iteration without a RuntimeError.
    2. ``get_by_id`` / ``delete`` coerce string IDs to UUID when the stored
       key is a UUID, matching the production code that calls
       ``get_by_id(str(uuid))``.
    """
    Base = new_mock_dao_class(dto_model=dto_model, id_field=id_field)

    class SafeDao(Base):
        def _resolve_id(self, id_: Any) -> Any:
            """If *id_* is a string and a UUID key exists, return that key."""
            if isinstance(id_, str) and id_ not in self.resources:
                try:
                    uuid_key = UUID(id_)
                    if uuid_key in self.resources:
                        return uuid_key
                except ValueError:
                    pass
            return id_

        async def get_by_id(self, id_: Any):
            return await super().get_by_id(self._resolve_id(id_))

        async def delete(self, id_: Any) -> None:
            await super().delete(self._resolve_id(id_))

        async def find_all(  # type: ignore[override]
            self, *, mapping: Mapping[str, Any]
        ) -> AsyncIterator:
            results = [item async for item in super().find_all(mapping=mapping)]
            for item in results:
                yield item

    return SafeDao


# Mock DAO classes
StudyDao = _safe_mock_dao_class(dto_model=Study, id_field="id")
MetadataDao = _safe_mock_dao_class(dto_model=ExperimentalMetadata, id_field="id")
PublicationDao = _safe_mock_dao_class(dto_model=Publication, id_field="id")
DacDao = _safe_mock_dao_class(dto_model=DataAccessCommittee, id_field="id")
DapDao = _safe_mock_dao_class(dto_model=DataAccessPolicy, id_field="id")
DatasetDao = _safe_mock_dao_class(dto_model=Dataset, id_field="id")
ResourceTypeDao = _safe_mock_dao_class(dto_model=ResourceType, id_field="id")
AccessionDao = _safe_mock_dao_class(dto_model=Accession, id_field="id")
AltAccessionDao = _safe_mock_dao_class(dto_model=AltAccession, id_field="id")
EmAccessionMapDao = _safe_mock_dao_class(dto_model=EmAccessionMap, id_field="id")

# Standard test user UUIDs
USER_STEWARD = UUID("00000000-0000-0000-0000-000000000001")
USER_SUBMITTER = UUID("00000000-0000-0000-0000-000000000002")
USER_OTHER = UUID("00000000-0000-0000-0000-000000000099")


@pytest.fixture(name="config")
def config_fixture() -> ConfigFixture:
    """Generate a fresh JWK and return a ConfigFixture with matching auth_key."""
    jwk = generate_jwk()
    auth_key = jwk.export(private_key=False)
    config = get_config(auth_key=auth_key)
    return ConfigFixture(config=config, jwk=jwk)


@pytest.fixture()
def study_dao():
    return StudyDao()


@pytest.fixture()
def metadata_dao():
    return MetadataDao()


@pytest.fixture()
def publication_dao():
    return PublicationDao()


@pytest.fixture()
def dac_dao():
    return DacDao()


@pytest.fixture()
def dap_dao():
    return DapDao()


@pytest.fixture()
def dataset_dao():
    return DatasetDao()


@pytest.fixture()
def resource_type_dao():
    return ResourceTypeDao()


@pytest.fixture()
def accession_dao():
    return AccessionDao()


@pytest.fixture()
def alt_accession_dao():
    return AltAccessionDao()


@pytest.fixture()
def em_accession_map_dao():
    return EmAccessionMapDao()


@pytest.fixture()
def event_store():
    return InMemEventStore()


@pytest.fixture()
def event_publisher(event_store):
    provider = InMemEventPublisher(event_store=event_store)
    return EventPubTranslator(config=EventPubConfig(), provider=provider)


@pytest.fixture()
def data_access(dac_dao, dap_dao, dataset_dao):
    """Create a DataAccessController wired to in-memory DAOs."""
    return DataAccessController(
        dac_dao=dac_dao,
        dap_dao=dap_dao,
        dataset_dao=dataset_dao,
    )


@pytest.fixture()
def study_controller(
    study_dao,
    metadata_dao,
    publication_dao,
    dataset_dao,
    accession_dao,
    em_accession_map_dao,
    event_publisher,
    data_access,
):
    """Create a StudyController wired to in-memory DAOs."""
    return StudyController(
        study_dao=study_dao,
        metadata_dao=metadata_dao,
        publication_dao=publication_dao,
        dataset_dao=dataset_dao,
        accession_dao=accession_dao,
        em_accession_map_dao=em_accession_map_dao,
        event_publisher=event_publisher,
        data_access=data_access,
    )


@pytest.fixture()
def dataset_controller(dataset_dao, study_dao, accession_dao, data_access):
    """Create a DatasetController wired to in-memory DAOs."""
    return DatasetController(
        dataset_dao=dataset_dao,
        study_dao=study_dao,
        accession_dao=accession_dao,
        data_access=data_access,
    )


@pytest.fixture()
def metadata_controller(study_dao, metadata_dao):
    """Create a MetadataController wired to in-memory DAOs."""
    return MetadataController(
        study_dao=study_dao,
        metadata_dao=metadata_dao,
    )


@pytest.fixture()
def publication_controller(study_dao, publication_dao, accession_dao):
    """Create a PublicationController wired to in-memory DAOs."""
    return PublicationController(
        study_dao=study_dao,
        publication_dao=publication_dao,
        accession_dao=accession_dao,
    )


@pytest.fixture()
def filename_controller(
    study_dao,
    metadata_dao,
    accession_dao,
    alt_accession_dao,
    em_accession_map_dao,
    event_publisher,
):
    """Create a FilenameController wired to in-memory DAOs."""
    return FilenameController(
        study_dao=study_dao,
        metadata_dao=metadata_dao,
        accession_dao=accession_dao,
        alt_accession_dao=alt_accession_dao,
        em_accession_map_dao=em_accession_map_dao,
        event_publisher=event_publisher,
    )


@pytest.fixture()
def resource_type_controller(resource_type_dao, study_dao, dataset_dao):
    """Create a ResourceTypeController wired to in-memory DAOs."""
    return ResourceTypeController(
        resource_type_dao=resource_type_dao,
        study_dao=study_dao,
        dataset_dao=dataset_dao,
    )


@pytest.fixture()
def controller(
    study_controller,
    dataset_controller,
    metadata_controller,
    publication_controller,
    filename_controller,
    resource_type_controller,
    data_access,
    accession_dao,
    alt_accession_dao,
):
    """Create a StudyRegistryController wired to in-memory DAOs."""
    accession_controller = AccessionController(
        accession_dao=accession_dao,
        alt_accession_dao=alt_accession_dao,
    )
    return StudyRegistryController(
        accession_controller=accession_controller,
        study_controller=study_controller,
        dataset_controller=dataset_controller,
        metadata_controller=metadata_controller,
        publication_controller=publication_controller,
        filename_controller=filename_controller,
        resource_type_controller=resource_type_controller,
        data_access=data_access,
    )
