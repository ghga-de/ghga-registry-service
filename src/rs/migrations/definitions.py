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

from hexkit.providers.mongodb.migrations import MigrationDefinition, Reversible
from hexkit.utils import now_utc_ms_prec

from rs.constants import FILE_ACCESSION_COLLECTION, RESEARCH_DATA_UPLOAD_BOX_COLLECTION
from rs.core.models import FileAccession

log = logging.getLogger(__name__)

# The pre-migration collection names (the new names are FILE_ACCESSION_COLLECTION
# and RESEARCH_DATA_UPLOAD_BOX_COLLECTION respectively).
V1_FILE_ACCESSION_MAPPING_COLLECTION = "fileAccessionMappings"
V1_BOX_COLLECTION = "boxes"


class V2Migration(MigrationDefinition, Reversible):
    """Rename and reshape the file-accession and box collections.

    1. Replace the `fileAccessionMappings` collection with `fileAccessions`.

       The old collection stored `FileAccessionMapping` outbox documents keyed by
       the accession string (`_id`) with a `file_id`. The new collection stores the
       richer `FileAccession` model, able to also track unmapped accessions and
       studies. For each existing (already-mapped) document:
       - `_id` (the accession) is kept and becomes the new `pid`
       - `file_id` is carried over
       - `study_id` is set to None (unknown for legacy records)
       - `created` and `mapped` are set to the migration run time
       - the outbox `__metadata__` is preserved (publication state is retained)

    2. Rename the `boxes` collection to `researchDataUploadBoxes`.

       The collection stores `ResearchDataUploadBox` outbox documents. Only the
       collection name changes; the document schema is unchanged, so each document
       (including its `_id` and outbox `__metadata__`) is copied over verbatim.

    The old `fileAccessionMappings` and `boxes` collections are dropped afterwards.

    Reversal is best-effort: the box rename is fully reversible, but the file
    accession reversal only recovers the original `FileAccessionMapping` fields
    (`_id` and `file_id`). The backfilled `created`/`mapped` timestamps and the
    `study_id` are dropped, and unmapped accessions (those with a null `file_id`,
    which had no representation in the old schema) are discarded.
    """

    version = 2

    async def apply(self):
        """Perform the migration."""
        collection_names = await self._db.list_collection_names()

        await self._migrate_file_accessions(collection_names)
        await self._migrate_boxes(collection_names)

    async def unapply(self):
        """Reverse the migration (best-effort)."""
        collection_names = await self._db.list_collection_names()

        await self._unmigrate_file_accessions(collection_names)
        await self._unmigrate_boxes(collection_names)

    async def _migrate_file_accessions(self, collection_names: list[str]):
        """Replace the `fileAccessionMappings` collection with `fileAccessions`."""
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

    async def _migrate_boxes(self, collection_names: list[str]):
        """Rename the `boxes` collection to `researchDataUploadBoxes`."""
        if V1_BOX_COLLECTION not in collection_names:
            log.info(
                "Collection '%s' not found, nothing to migrate %s",
                V1_BOX_COLLECTION,
                self._log_blurb,
            )
            return

        old_collection = self._db[V1_BOX_COLLECTION]
        new_collection = self._db[RESEARCH_DATA_UPLOAD_BOX_COLLECTION]

        async for doc in old_collection.find():
            await new_collection.insert_one(doc)

        await old_collection.drop()
        log.info(
            "Migrated collection '%s' to '%s' %s",
            V1_BOX_COLLECTION,
            RESEARCH_DATA_UPLOAD_BOX_COLLECTION,
            self._log_blurb,
        )

    async def _unmigrate_file_accessions(self, collection_names: list[str]):
        """Restore the `fileAccessionMappings` collection from `fileAccessions`.

        Only the original `FileAccessionMapping` fields (`_id` and `file_id`) are
        recovered; `study_id`, `created` and `mapped` are dropped. Unmapped
        accessions (with a null `file_id`) had no representation in the old schema
        and are discarded.
        """
        if FILE_ACCESSION_COLLECTION not in collection_names:
            log.info(
                "Collection '%s' not found, nothing to migrate %s",
                FILE_ACCESSION_COLLECTION,
                self._log_blurb,
            )
            return

        new_collection = self._db[FILE_ACCESSION_COLLECTION]
        old_collection = self._db[V1_FILE_ACCESSION_MAPPING_COLLECTION]

        async for doc in new_collection.find():
            if doc.get("file_id") is None:
                continue
            old_doc = {"_id": doc["_id"], "file_id": doc["file_id"]}
            metadata = doc.get("__metadata__")
            if metadata is not None:
                old_doc["__metadata__"] = metadata
            await old_collection.insert_one(old_doc)

        await new_collection.drop()
        log.info(
            "Reverted collection '%s' to '%s' %s",
            FILE_ACCESSION_COLLECTION,
            V1_FILE_ACCESSION_MAPPING_COLLECTION,
            self._log_blurb,
        )

    async def _unmigrate_boxes(self, collection_names: list[str]):
        """Rename the `researchDataUploadBoxes` collection back to `boxes`."""
        if RESEARCH_DATA_UPLOAD_BOX_COLLECTION not in collection_names:
            log.info(
                "Collection '%s' not found, nothing to migrate %s",
                RESEARCH_DATA_UPLOAD_BOX_COLLECTION,
                self._log_blurb,
            )
            return

        new_collection = self._db[RESEARCH_DATA_UPLOAD_BOX_COLLECTION]
        old_collection = self._db[V1_BOX_COLLECTION]

        async for doc in new_collection.find():
            await old_collection.insert_one(doc)

        await new_collection.drop()
        log.info(
            "Reverted collection '%s' to '%s' %s",
            RESEARCH_DATA_UPLOAD_BOX_COLLECTION,
            V1_BOX_COLLECTION,
            self._log_blurb,
        )
