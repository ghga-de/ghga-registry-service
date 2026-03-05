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

"""Core implementation of Experimental Metadata operations."""

import logging

from ghga_service_commons.utils.utc_dates import now_as_utc
from hexkit.protocols.dao import ResourceNotFoundError

from srs.core.models import ExperimentalMetadata
from srs.core.utils import get_study_or_raise, require_pending
from srs.ports.inbound.metadata import MetadataPort
from srs.ports.outbound.dao import ExperimentalMetadataDao, StudyDao

log = logging.getLogger(__name__)


class MetadataController(MetadataPort):
    """Core implementation of Experimental Metadata operations."""

    def __init__(
        self,
        *,
        study_dao: StudyDao,
        metadata_dao: ExperimentalMetadataDao,
    ):
        self._study_dao = study_dao
        self._metadata_dao = metadata_dao

    # --- Metadata operations ---

    async def upsert_metadata(
        self, *, study_id: str, metadata: dict
    ) -> None:
        """Create or update experimental metadata for a study."""
        study = await get_study_or_raise(self._study_dao, study_id)
        await require_pending(study)

        now = now_as_utc()
        em = ExperimentalMetadata(
            id=study_id,
            metadata=metadata,
            submitted=now,
        )
        try:
            await self._metadata_dao.update(em)
        except ResourceNotFoundError:
            await self._metadata_dao.insert(em)
        log.info("Upserted experimental metadata for study %s", study_id)

    async def get_metadata(self, *, study_id: str) -> ExperimentalMetadata:
        """Get experimental metadata for a study."""
        try:
            return await self._metadata_dao.get_by_id(study_id)
        except ResourceNotFoundError as err:
            raise self.MetadataNotFoundError(study_id=study_id) from err

    async def delete_metadata(self, *, study_id: str) -> None:
        """Delete experimental metadata for a study."""
        study = await get_study_or_raise(self._study_dao, study_id)
        await require_pending(study)
        try:
            await self._metadata_dao.delete(study_id)
        except ResourceNotFoundError as err:
            raise self.MetadataNotFoundError(study_id=study_id) from err
        log.info("Deleted experimental metadata for study %s", study_id)
