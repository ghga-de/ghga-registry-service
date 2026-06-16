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

"""Tests for the database migration logic."""

from datetime import datetime
from uuid import uuid4

import pytest
from hexkit.providers.mongodb.testutils import MongoDbFixture

from rs.constants import FILE_ACCESSION_COLLECTION
from rs.migrations import run_db_migrations
from rs.migrations.definitions import V1_FILE_ACCESSION_MAPPING_COLLECTION
from tests.fixtures.config import get_config

pytestmark = pytest.mark.asyncio()


async def test_v2_migration(mongodb: MongoDbFixture):
    """The v2 migration replaces the `fileAccessionMappings` collection with the
    richer `fileAccessions` collection, backfilling the new fields.
    """
    config = get_config(sources=[mongodb.config])
    db = mongodb.client[config.db_name]
    old_collection = db[V1_FILE_ACCESSION_MAPPING_COLLECTION]
    new_collection = db[FILE_ACCESSION_COLLECTION]

    file_id1, file_id2 = uuid4(), uuid4()
    correlation_id = str(uuid4())

    # Seed legacy FileAccessionMapping outbox documents
    old_collection.delete_many({})
    old_collection.insert_many(
        [
            {
                "_id": "GHGAF001",
                "file_id": file_id1,
                "__metadata__": {
                    "deleted": False,
                    "published": True,
                    "correlation_id": correlation_id,
                    "last_event_id": None,
                },
            },
            {
                "_id": "GHGAF002",
                "file_id": file_id2,
                "__metadata__": {
                    "deleted": False,
                    "published": True,
                    "correlation_id": correlation_id,
                    "last_event_id": None,
                },
            },
        ]
    )

    # Run the migration
    await run_db_migrations(config=config, target_version=2)

    # The old collection is gone, the new one took over
    collection_names = db.list_collection_names()
    assert V1_FILE_ACCESSION_MAPPING_COLLECTION not in collection_names
    assert FILE_ACCESSION_COLLECTION in collection_names

    migrated = sorted(new_collection.find({}).to_list(), key=lambda d: d["_id"])
    assert [d["_id"] for d in migrated] == ["GHGAF001", "GHGAF002"]

    for doc, file_id in zip(migrated, [file_id1, file_id2], strict=True):
        assert doc["file_id"] == file_id
        assert doc["study_id"] is None
        assert isinstance(doc["created"], datetime)
        assert isinstance(doc["mapped"], datetime)
        # Outbox metadata is preserved (records stay published)
        assert doc["__metadata__"]["published"] is True
        assert doc["__metadata__"]["correlation_id"] == correlation_id


async def test_v2_migration_no_old_collection(mongodb: MongoDbFixture):
    """The migration is a no-op when the legacy collection does not exist."""
    config = get_config(sources=[mongodb.config])
    db = mongodb.client[config.db_name]
    db[V1_FILE_ACCESSION_MAPPING_COLLECTION].drop()

    # Should complete without error and not create the new collection
    await run_db_migrations(config=config, target_version=2)

    assert V1_FILE_ACCESSION_MAPPING_COLLECTION not in db.list_collection_names()
