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

"""In-memory mock DAO and event publisher for unit testing."""

from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any, Generic, TypeVar

from hexkit.protocols.dao import (
    MultipleHitsFoundError,
    NoHitsFoundError,
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
)
from pydantic import BaseModel

from srs.core.models import AnnotatedExperimentalMetadata
from srs.ports.outbound.event_pub import EventPublisherPort

T = TypeVar("T", bound=BaseModel)


class InMemoryDao(Generic[T]):
    """In-memory DAO implementation matching the hexkit Dao protocol.

    Stores dto instances keyed by a configurable ``id_field``.
    """

    def __init__(self, *, id_field: str = "id"):
        self._store: dict[str, T] = {}
        self._id_field = id_field

    def _get_id(self, dto: T) -> str:
        return str(getattr(dto, self._id_field))

    # -- protocol methods --

    @classmethod
    @asynccontextmanager
    async def with_transaction(cls):
        """No-op transaction context manager."""
        yield cls()

    async def get_by_id(self, id_: str) -> T:
        try:
            return self._store[str(id_)]
        except KeyError:
            raise ResourceNotFoundError(id_=str(id_))

    async def update(self, dto: T) -> None:
        key = self._get_id(dto)
        if key not in self._store:
            raise ResourceNotFoundError(id_=key)
        self._store[key] = dto

    async def delete(self, id_: str) -> None:
        key = str(id_)
        if key not in self._store:
            raise ResourceNotFoundError(id_=key)
        del self._store[key]

    async def find_one(self, *, mapping: Mapping[str, Any]) -> T:
        hits = [dto for dto in self._store.values() if self._matches(dto, mapping)]
        if len(hits) == 0:
            raise NoHitsFoundError(mapping=mapping)
        if len(hits) > 1:
            raise MultipleHitsFoundError(mapping=mapping)
        return hits[0]

    def find_all(self, *, mapping: Mapping[str, Any]) -> AsyncIterator[T]:  # type: ignore[override]
        return _async_iter(
            [dto for dto in self._store.values() if self._matches(dto, mapping)]
        )

    async def insert(self, dto: T) -> None:
        key = self._get_id(dto)
        if key in self._store:
            raise ResourceAlreadyExistsError(id_=key)
        self._store[key] = dto

    async def upsert(self, dto: T) -> None:
        self._store[self._get_id(dto)] = dto

    # -- helpers --

    def _matches(self, dto: T, mapping: Mapping[str, Any]) -> bool:
        for field, value in mapping.items():
            dto_val = getattr(dto, field, None)
            # Handle enum values (compare by value)
            dto_cmp = dto_val.value if hasattr(dto_val, "value") else dto_val
            if str(dto_cmp) != str(value):
                return False
        return True

    @property
    def data(self) -> dict[str, T]:
        """Expose the internal store for assertions."""
        return self._store


async def _async_iter(items: list[T]) -> AsyncIterator[T]:
    """Helper to turn a list into an async iterator."""
    for item in items:
        yield item


class InMemoryEventPublisher(EventPublisherPort):
    """In-memory event publisher that records published events."""

    def __init__(self):
        self.annotated_metadata_events: list[AnnotatedExperimentalMetadata] = []
        self.file_id_mapping_events: list[dict[str, str]] = []

    async def publish_annotated_metadata(
        self, *, payload: AnnotatedExperimentalMetadata
    ) -> None:
        self.annotated_metadata_events.append(payload)

    async def publish_file_id_mapping(
        self, *, mapping: dict[str, str]
    ) -> None:
        self.file_id_mapping_events.append(mapping)
