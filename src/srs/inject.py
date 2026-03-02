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

"""Dependency injection wiring for the Study Registry Service."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from ghga_service_commons.auth.ghga import AuthContext, JWTAuthContextProvider
from hexkit.providers.akafka import KafkaEventPublisher
from hexkit.providers.mongodb import MongoDbDaoFactory

from srs.adapters.inbound.fastapi_ import dummies
from srs.adapters.inbound.fastapi_.configure import get_configured_app
from srs.adapters.inbound.fastapi_.http_authorization import AuthProviderBundle
from srs.adapters.outbound.dao import (
    get_accession_dao,
    get_alt_accession_dao,
    get_dac_dao,
    get_dap_dao,
    get_dataset_dao,
    get_em_accession_map_dao,
    get_metadata_dao,
    get_publication_dao,
    get_resource_type_dao,
    get_study_dao,
)
from srs.adapters.outbound.event_pub import EventPubTranslator
from srs.config import Config
from srs.core.study_registry import StudyRegistryController
from srs.ports.inbound.study_registry import StudyRegistryPort


@asynccontextmanager
async def prepare_core(
    *, config: Config
) -> AsyncGenerator[StudyRegistryPort, None]:
    """Set up the core controller with all outbound adapters."""
    dao_factory = MongoDbDaoFactory(config=config)

    study_dao = await get_study_dao(dao_factory=dao_factory)
    metadata_dao = await get_metadata_dao(dao_factory=dao_factory)
    publication_dao = await get_publication_dao(dao_factory=dao_factory)
    dac_dao = await get_dac_dao(dao_factory=dao_factory)
    dap_dao = await get_dap_dao(dao_factory=dao_factory)
    dataset_dao = await get_dataset_dao(dao_factory=dao_factory)
    resource_type_dao = await get_resource_type_dao(dao_factory=dao_factory)
    accession_dao = await get_accession_dao(dao_factory=dao_factory)
    alt_accession_dao = await get_alt_accession_dao(dao_factory=dao_factory)
    em_accession_map_dao = await get_em_accession_map_dao(
        dao_factory=dao_factory
    )

    async with KafkaEventPublisher.construct(config=config) as kafka_publisher:
        event_publisher = EventPubTranslator(kafka_publisher=kafka_publisher)

        controller = StudyRegistryController(
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
        yield controller


@asynccontextmanager
async def prepare_rest_app(
    *, config: Config
) -> AsyncGenerator[FastAPI, None]:
    """Prepare the complete FastAPI application with DI wiring."""
    app = get_configured_app(config=config)

    async with prepare_core(config=config) as controller:
        auth_context_provider = JWTAuthContextProvider[AuthContext](
            config=config,
            context_class=AuthContext,
        )
        auth_bundle = AuthProviderBundle(
            context_provider=auth_context_provider
        )

        app.dependency_overrides[dummies.study_registry_port] = (
            lambda: controller
        )
        app.dependency_overrides[dummies.auth_provider] = lambda: auth_bundle

        yield app
