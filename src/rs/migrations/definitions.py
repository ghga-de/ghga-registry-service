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

"""Database migration logic for the rs service."""

import logging

from hexkit.providers.mongodb.migrations import MigrationDefinition
from hexkit.utils import now_utc_ms_prec

from rs.constants import FILE_ACCESSION_COLLECTION
from rs.core.models import FileAccession

log = logging.getLogger(__name__)

# The pre-migration collection name (the new name is FILE_ACCESSION_COLLECTION).
V1_FILE_ACCESSION_MAPPING_COLLECTION = "fileAccessionMappings"


class V2Migration(MigrationDefinition):
    """Replace the `fileAccessionMappings` collection with `fileAccessions`.

    The old collection stored `FileAccessionMapping` outbox documents keyed by the
    accession string (`_id`) with a `file_id`. The new collection stores the richer
    `FileAccession` model, able to also track unmapped accessions and studies.

    For each existing (already-mapped) document:
    - `_id` (the accession) is kept and becomes the new `pid`
    - `file_id` is carried over
    - `study_id` is set to None (unknown for legacy records)
    - `created` and `mapped` are set to the migration run time
    - the outbox `__metadata__` is preserved (publication state is retained)

    The old `fileAccessionMappings` collection is dropped afterwards.

    This migration is not reversible because the backfilled `created`/`mapped`
    timestamps and the dropped `study_id` information cannot be recovered.
    """

    version = 2

    async def apply(self):
        """Perform the migration."""
        collection_names = await self._db.list_collection_names()
        if V1_FILE_ACCESSION_MAPPING_COLLECTION not in collection_names:
            log.info(
                "Collection '%s' not found, nothing to migrate %s",
                V1_FILE_ACCESSION_MAPPING_COLLECTION,
                self._log_blurb,
            )
            return

        old_collection = self._db[V1_FILE_ACCESSION_MAPPING_COLLECTION]
        new_collection = self._db[FILE_ACCESSION_COLLECTION]
        now = now_utc_ms_prec()

        async for doc in old_collection.find():
            metadata = doc.get("__metadata__")
            file_accession = FileAccession(
                pid=doc["_id"],
                file_id=doc.get("file_id"),
                study_id=None,
                created=now,
                mapped=now,
            )
            new_doc = file_accession.model_dump()
            new_doc["_id"] = new_doc.pop("pid")
            new_doc["__metadata__"] = metadata or {
                "deleted": False,
                "published": True,
                "correlation_id": "",
                "last_event_id": None,
            }
            await new_collection.insert_one(new_doc)

        await old_collection.drop()
        log.info(
            "Migrated collection '%s' to '%s' %s",
            V1_FILE_ACCESSION_MAPPING_COLLECTION,
            FILE_ACCESSION_COLLECTION,
            self._log_blurb,
        )
