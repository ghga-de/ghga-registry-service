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

from hexkit.custom_types import JsonObject
from hexkit.protocols.eventsub import EventSubscriberProtocol
from hexkit.providers.akafka import KafkaEventPublisher

from srs.core.models import AnnotatedExperimentalMetadata
from srs.ports.outbound.event_pub import EventPublisherPort


class EventPubTranslator(EventPublisherPort):
    """Translates outbound events into Kafka messages."""

    ANNOTATED_METADATA_TOPIC = "annotated_metadata"
    ANNOTATED_METADATA_TYPE = "annotated_metadata_created"

    FILE_ID_MAPPING_TOPIC = "file_id_mapping"
    FILE_ID_MAPPING_TYPE = "file_id_mapping_created"

    def __init__(self, *, kafka_publisher: KafkaEventPublisher):
        self._publisher = kafka_publisher

    async def publish_annotated_metadata(
        self, *, payload: AnnotatedExperimentalMetadata
    ) -> None:
        """Publish annotated metadata event to Kafka."""
        await self._publisher.publish(
            payload=json.loads(payload.model_dump_json()),
            topic=self.ANNOTATED_METADATA_TOPIC,
            type_=self.ANNOTATED_METADATA_TYPE,
            key=payload.study.id,
        )

    async def publish_file_id_mapping(
        self, *, mapping: dict[str, str]
    ) -> None:
        """Publish file ID mapping event to Kafka."""
        await self._publisher.publish(
            payload={"mapping": mapping},
            topic=self.FILE_ID_MAPPING_TOPIC,
            type_=self.FILE_ID_MAPPING_TYPE,
            key="file_id_mapping",
        )
