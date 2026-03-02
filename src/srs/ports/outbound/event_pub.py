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

"""Outbound event publisher port definition."""

from abc import ABC, abstractmethod

from srs.core.models import AnnotatedExperimentalMetadata


class EventPublisherPort(ABC):
    """Port for publishing events to the message broker."""

    @abstractmethod
    async def publish_annotated_metadata(
        self, *, payload: AnnotatedExperimentalMetadata
    ) -> None:
        """Publish an AnnotatedExperimentalMetadata event."""
        ...

    @abstractmethod
    async def publish_file_id_mapping(
        self, *, mapping: dict[str, str]
    ) -> None:
        """Republish a file accession to internal file ID mapping."""
        ...
