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

"""Core implementation of Filename operations."""

import logging

from ghga_service_commons.utils.utc_dates import now_as_utc
from hexkit.protocols.dao import ResourceNotFoundError

from srs.core.models import AltAccession, AltAccessionType
from srs.core.utils import get_or_raise, get_study_or_raise
from srs.ports.inbound.filename import FilenamePort
from srs.ports.outbound.dao import (
    AccessionDao,
    AltAccessionDao,
    EmAccessionMapDao,
    ExperimentalMetadataDao,
    StudyDao,
)
from srs.ports.outbound.event_pub import EventPublisherPort

log = logging.getLogger(__name__)


class FilenameController(FilenamePort):
    """Core implementation of Filename operations."""

    def __init__(
        self,
        *,
        study_dao: StudyDao,
        metadata_dao: ExperimentalMetadataDao,
        accession_dao: AccessionDao,
        alt_accession_dao: AltAccessionDao,
        em_accession_map_dao: EmAccessionMapDao,
        event_publisher: EventPublisherPort,
    ):
        self._study_dao = study_dao
        self._metadata_dao = metadata_dao
        self._accession_dao = accession_dao
        self._alt_accession_dao = alt_accession_dao
        self._em_accession_map_dao = em_accession_map_dao
        self._event_publisher = event_publisher

    # --- Filename operations ---

    async def get_filenames(
        self, *, study_id: str
    ) -> dict[str, dict[str, str]]:
        """Get file accession to filename/alias mapping for a study."""
        await get_study_or_raise(self._study_dao, study_id)

        # Get the EM accession map for files
        em_map = await get_or_raise(self._em_accession_map_dao, study_id, self.MetadataNotFoundError(study_id=study_id))

        # Get the experimental metadata to extract file names/aliases
        em = await self._metadata_dao.get_by_id(study_id)
        metadata = em.metadata

        result: dict[str, dict[str, str]] = {}
        file_accessions = em_map.maps.get("files", {})
        files_data = metadata.get("files", {})

        for alias, accession_id in file_accessions.items():
            file_info = files_data.get(alias, {})
            name = (
                file_info.get("name", alias)
                if isinstance(file_info, dict)
                else alias
            )
            result[accession_id] = {"name": name, "alias": alias}

        return result

    async def post_filenames(
        self, *, study_id: str, file_id_map: dict[str, str]
    ) -> None:
        """Store file accession to internal file ID mappings."""
        await get_study_or_raise(self._study_dao, study_id)

        now = now_as_utc()
        for pid, file_id in file_id_map.items():
            # Verify the file accession exists
            try:
                await self._accession_dao.get_by_id(pid)
            except ResourceNotFoundError as err:
                raise self.ValidationError(
                    detail=f"File accession {pid} does not exist."
                ) from err

            # Upsert AltAccession with type FILE_ID
            alt = AltAccession(
                id=file_id,
                pid=pid,
                type=AltAccessionType.FILE_ID,
                created=now,
            )
            try:
                await self._alt_accession_dao.update(alt)
            except ResourceNotFoundError:
                await self._alt_accession_dao.insert(alt)

        # Republish mapping as event
        await self._event_publisher.publish_file_id_mapping(
            mapping=file_id_map
        )
        log.info(
            "Stored %d file ID mappings for study %s",
            len(file_id_map),
            study_id,
        )
