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

"""Outbound adapter: Kafka event publisher for SRS."""

import json

from hexkit.protocols.eventpub import EventPublisherProtocol
from pydantic import Field
from pydantic_settings import BaseSettings

from srs.core.models import AnnotatedExperimentalMetadata
from srs.ports.outbound.event_pub import EventPublisherPort


class EventPubConfig(BaseSettings):
    """Config for event publishing topics and types."""

    annotated_metadata_topic: str = Field(
        default="annotated_metadata",
        description="Kafka topic for annotated metadata events.",
    )
    annotated_metadata_type: str = Field(
        default="annotated_metadata_created",
        description="Event type for annotated metadata events.",
    )
    file_id_mapping_topic: str = Field(
        default="file_id_mapping",
        description="Kafka topic for file ID mapping events.",
    )
    file_id_mapping_type: str = Field(
        default="file_id_mapping_created",
        description="Event type for file ID mapping events.",
    )


class EventPubTranslator(EventPublisherPort):
    """Translates outbound events into Kafka messages."""

    def __init__(
        self,
        *,
        config: EventPubConfig,
        provider: EventPublisherProtocol,
    ):
        self._config = config
        self._provider = provider

    async def publish_annotated_metadata(
        self, *, payload: AnnotatedExperimentalMetadata
    ) -> None:
        """Publish annotated metadata event to Kafka."""
        await self._provider.publish(
            payload=json.loads(payload.model_dump_json()),
            topic=self._config.annotated_metadata_topic,
            type_=self._config.annotated_metadata_type,
            key=payload.study.id,
        )

    async def publish_file_id_mapping(
        self, *, mapping: dict[str, str]
    ) -> None:
        """Publish file ID mapping event to Kafka."""
        await self._provider.publish(
            payload={"mapping": mapping},
            topic=self._config.file_id_mapping_topic,
            type_=self._config.file_id_mapping_type,
            key="file_id_mapping",
        )
