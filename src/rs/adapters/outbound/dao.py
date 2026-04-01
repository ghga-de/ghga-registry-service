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

"""Outbound DAO adapter — wires DTO models to MongoDB/Kafka via the outbox pattern."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from ghga_event_schemas.configs import ResearchDataUploadBoxEventsConfig
from hexkit.providers.mongodb import MongoDbIndex
from hexkit.providers.mongokafka import MongoKafkaDaoPublisherFactory

from rs.constants import BOX_COLLECTION
from rs.core.models import AltAccession, ResearchDataUploadBox
from rs.ports.outbound.dao import AltAccessionDao, BoxDao

__all__ = ["OutboxPubConfig", "get_alt_accession_dao", "get_box_dao"]


class OutboxPubConfig(ResearchDataUploadBoxEventsConfig):
    """Config needed to publish outbox events"""


@asynccontextmanager
async def get_alt_accession_dao(
    *,
    config,
    dao_publisher_factory: MongoKafkaDaoPublisherFactory,
) -> AsyncGenerator[AltAccessionDao]:
    """Construct an AltAccession DAO from the shared factory."""
    alt_accession_dao = await dao_publisher_factory.get_dao(
        name=config.alt_accessions_collection,
        dto_model=AltAccession,
        id_field="pid",
        dto_to_event=lambda event: event.model_dump(mode="json"),
        event_topic=config.alt_accessions_topic,
        autopublish=True,
    )
    yield alt_accession_dao


async def get_box_dao(
    *, config: OutboxPubConfig, dao_publisher_factory: MongoKafkaDaoPublisherFactory
) -> BoxDao:
    """Construct a ResearchDataUploadBox outbox DAO from the provided dao_publisher_factory"""
    if not dao_publisher_factory:
        raise RuntimeError("No DAO Factory and no override provided for BoxDao")

    return await dao_publisher_factory.get_dao(
        name=BOX_COLLECTION,
        dto_model=ResearchDataUploadBox,
        id_field="id",
        autopublish=True,
        dto_to_event=lambda dto: dto.model_dump(mode="json"),
        event_topic=config.research_data_upload_box_topic,
        indexes=[MongoDbIndex(fields="file_upload_box_id")],
    )
