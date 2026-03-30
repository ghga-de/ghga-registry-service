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

"""Bundle different test fixtures into one fixture."""

from collections.abc import AsyncGenerator
from dataclasses import dataclass

import pytest_asyncio
from ghga_service_commons.api.testing import AsyncTestClient
from ghga_service_commons.utils import jwt_helpers
from hexkit.providers.akafka.testutils import KafkaFixture
from hexkit.providers.mongodb.testutils import MongoDbFixture
from jwcrypto.jwk import JWK

from rs.config import Config
from rs.inject import prepare_core, prepare_rest_app
from rs.ports.inbound.orchestrator import UploadOrchestratorPort
from tests.fixtures.config import get_config


@dataclass
class JointFixture:
    """Returned by the `joint_fixture`."""

    config: Config
    orchestrator: UploadOrchestratorPort
    kafka: KafkaFixture
    mongodb: MongoDbFixture
    rest_client: AsyncTestClient
    auth_jwk: JWK


@pytest_asyncio.fixture(scope="function")
async def joint_fixture(
    mongodb: MongoDbFixture,
    kafka: KafkaFixture,
) -> AsyncGenerator[JointFixture]:
    """A fixture that embeds all other fixtures for API-level integration testing."""
    auth_jwk = jwt_helpers.generate_jwk()
    auth_key = auth_jwk.export(private_key=False)
    signing_jwk = jwt_helpers.generate_jwk()
    work_order_signing_key = signing_jwk.export(private_key=True)

    config = get_config(
        sources=[mongodb.config, kafka.config],
        auth_key=auth_key,
        work_order_signing_key=work_order_signing_key,
    )

    async with (
        prepare_core(config=config) as orchestrator,
        prepare_rest_app(
            config=config, upload_orchestrator_override=orchestrator
        ) as app,
        AsyncTestClient(app=app) as rest_client,
    ):
        yield JointFixture(
            config=config,
            orchestrator=orchestrator,
            kafka=kafka,
            mongodb=mongodb,
            rest_client=rest_client,
            auth_jwk=auth_jwk,
        )
