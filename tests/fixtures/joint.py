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
#

"""Bundle different test fixtures into one fixture."""

from collections.abc import AsyncGenerator
from dataclasses import dataclass

import pytest_asyncio
from ghga_service_commons.api.testing import AsyncTestClient
from hexkit.providers.akafka.testutils import KafkaFixture
from hexkit.providers.mongodb.testutils import MongoDbFixture
from jwcrypto.jwk import JWK

from srs.config import Config
from srs.inject import prepare_core, prepare_rest_app
from srs.ports.inbound.study_registry import StudyRegistryPort
from tests.fixtures import ConfigFixture
from tests.fixtures.config import get_config


@dataclass
class JointFixture:
    """Returned by the `joint_fixture`."""

    config: Config
    controller: StudyRegistryPort
    kafka: KafkaFixture
    mongodb: MongoDbFixture
    rest_client: AsyncTestClient
    jwk: JWK


@pytest_asyncio.fixture(scope="function")
async def joint_fixture(
    mongodb: MongoDbFixture,
    kafka: KafkaFixture,
    config: ConfigFixture,
) -> AsyncGenerator[JointFixture]:
    """A fixture that embeds all other fixtures for API-level integration testing.

    **Do not call directly** Instead, use get_joint_fixture().
    """
    # merge configs from different sources with the default one
    _config = get_config(
        sources=[config.config, mongodb.config, kafka.config],
    )

    # Knit together the components into the joint fixture
    async with (
        prepare_core(config=_config) as controller,
        prepare_rest_app(
            config=_config, controller_override=controller
        ) as app,
        AsyncTestClient(app=app) as rest_client,
    ):
        yield JointFixture(
            config=_config,
            controller=controller,
            kafka=kafka,
            mongodb=mongodb,
            rest_client=rest_client,
            jwk=config.jwk,
        )