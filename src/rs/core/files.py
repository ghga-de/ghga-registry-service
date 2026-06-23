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

"""FileController implementation"""

import logging

from hexkit.protocols.dao import ResourceNotFoundError
from hexkit.utils import now_utc_ms_prec
from pydantic import UUID4

from rs.core.models import PID, FileAccession
from rs.ports.inbound.files import FileControllerPort
from rs.ports.outbound.dao import FileAccessionDao

log = logging.getLogger(__name__)


class FileController(FileControllerPort):
    """Core implementation of file accession mapping and related operations."""

    def __init__(self, *, file_accession_dao: FileAccessionDao):
        self._file_accession_dao = file_accession_dao

    async def post_file_ids(
        self, *, study_id: str, file_id_map: dict[PID, UUID4]
    ) -> None:
        """Store file accession to internal file ID mappings.

        Raises:
            ConflictingAccessionError: If any accession in the map already exists in the
                DB with a different file_id. All conflicts are collected before raising
                so callers receive the full picture in a single error.
        """
        existing_mappings = {
            record.pid: record.file_id
            async for record in self._file_accession_dao.find_all(
                mapping={"pid": {"$in": list(file_id_map)}}
            )
        }
        conflicting_accessions = [
            accession
            for accession, requested_file_id in file_id_map.items()
            if existing_mappings.get(accession, requested_file_id) != requested_file_id
        ]

        if conflicting_accessions:
            raise self.ConflictingAccessionError(
                conflicting_accessions=conflicting_accessions
            )

        for accession, file_id in file_id_map.items():
            file_accession = FileAccession(
                pid=accession,
                file_id=file_id,
                study_id=study_id,
                mapped=now_utc_ms_prec(),
            )
            await self._file_accession_dao.upsert(file_accession)
            log.info(
                "Upserted file accession mapping for file ID %s pointing to"
                " accession %s for study %s.",
                file_id,
                accession,
                study_id,
            )

    async def register_unmapped_accessions(
        self, *, study_id: str, accessions: set[PID]
    ) -> None:
        """Ensure a FileAccession entry exists for each of the given accessions.

        Used to track file accessions discovered in legacy searchable resources before
        they have been mapped to internal file IDs. For each accession:

        - if no entry exists yet, a new unmapped entry (no file ID) is created, carrying
          the study ID;
        - if an unmapped entry already exists but carries a different study ID (including
          none yet), the study ID is updated.

        Entries that are already mapped to a file ID, or that already carry the same study
        ID, are left untouched. Since only unmapped entries (no file ID) are written, none
        of these writes publish an outbox event.
        """
        for accession in accessions:
            try:
                record = await self._file_accession_dao.get_by_id(accession)
            except ResourceNotFoundError:
                await self._file_accession_dao.insert(
                    FileAccession(pid=accession, study_id=study_id)
                )
                log.info(
                    "Created unmapped file accession %s for study %s.",
                    accession,
                    study_id,
                )
                continue
            if record.file_id is None and record.study_id != study_id:
                await self._file_accession_dao.upsert(
                    record.model_copy(update={"study_id": study_id})
                )
                log.info(
                    "Updated file accession %s to study %s.",
                    accession,
                    study_id,
                )

    async def get_study_ids_with_unmapped_accessions(self) -> set[str]:
        """Return the IDs of all studies that have at least one unmapped file accession.

        Queries the FileAccession records that have no file ID yet and collects the
        study IDs they are attributed to. Accessions without a study ID are skipped.
        """
        study_ids: set[str] = set()
        async for record in self._file_accession_dao.find_all(
            mapping={"file_id": None}
        ):
            if record.study_id is not None:
                study_ids.add(record.study_id)
        return study_ids

    async def get_accessions_by_file_ids(
        self, *, file_ids: set[UUID4]
    ) -> dict[UUID4, str]:
        """Query FileAccession records for the given file IDs.
        Returns a dict mapping file_id (UUID4) to accession (str).
        """
        result = {}
        async for record in self._file_accession_dao.find_all(
            mapping={"file_id": {"$in": list(file_ids)}}
        ):
            # The query only matches mapped records, so file_id is always set.
            if record.file_id is not None:
                result[record.file_id] = record.pid
        return result

    async def delete_mappings_for_file_ids(self, *, file_ids: set[UUID4]) -> None:
        """Delete FileAccessionMapping records for the given file IDs.

        Used when a ResearchDataUploadBox is deleted.
        """
        if not file_ids:
            return

        # Fetch the accessions, then delete each item.
        accessions_by_file_id = await self.get_accessions_by_file_ids(file_ids=file_ids)
        for file_id, accession in accessions_by_file_id.items():
            await self._file_accession_dao.delete(id_=accession)
            log.info(
                "Deleted file accession mapping for file ID %s (accession %s).",
                file_id,
                accession,
            )
