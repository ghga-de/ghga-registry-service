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

from uuid import UUID

import pytest

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
from srs.core.study_registry import StudyRegistryController
from tests.fixtures.mocks import InMemoryDao, InMemoryEventPublisher

# Standard test user UUIDs
USER_STEWARD = UUID("00000000-0000-0000-0000-000000000001")
USER_SUBMITTER = UUID("00000000-0000-0000-0000-000000000002")
USER_OTHER = UUID("00000000-0000-0000-0000-000000000099")


@pytest.fixture()
def study_dao():
    return InMemoryDao[Study]()


@pytest.fixture()
def metadata_dao():
    return InMemoryDao[ExperimentalMetadata]()


@pytest.fixture()
def publication_dao():
    return InMemoryDao[Publication]()


@pytest.fixture()
def dac_dao():
    return InMemoryDao[DataAccessCommittee]()


@pytest.fixture()
def dap_dao():
    return InMemoryDao[DataAccessPolicy]()


@pytest.fixture()
def dataset_dao():
    return InMemoryDao[Dataset]()


@pytest.fixture()
def resource_type_dao():
    return InMemoryDao[ResourceType]()


@pytest.fixture()
def accession_dao():
    return InMemoryDao[Accession]()


@pytest.fixture()
def alt_accession_dao():
    return InMemoryDao[AltAccession]()


@pytest.fixture()
def em_accession_map_dao():
    return InMemoryDao[EmAccessionMap]()


@pytest.fixture()
def event_publisher():
    return InMemoryEventPublisher()


@pytest.fixture()
def controller(
    study_dao,
    metadata_dao,
    publication_dao,
    dac_dao,
    dap_dao,
    dataset_dao,
    resource_type_dao,
    accession_dao,
    alt_accession_dao,
    em_accession_map_dao,
    event_publisher,
):
    """Create a StudyRegistryController wired to in-memory DAOs."""
    return StudyRegistryController(
        study_dao=study_dao,
        metadata_dao=metadata_dao,
        publication_dao=publication_dao,
        dac_dao=dac_dao,
        dap_dao=dap_dao,
        dataset_dao=dataset_dao,
        resource_type_dao=resource_type_dao,
        accession_dao=accession_dao,
        alt_accession_dao=alt_accession_dao,
        em_accession_map_dao=em_accession_map_dao,
        event_publisher=event_publisher,
    )
