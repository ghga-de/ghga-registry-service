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

"""Core implementation of Accession lookup operations."""

import logging

from hexkit.protocols.dao import ResourceNotFoundError

from srs.core.models import Accession, AltAccession, AltAccessionType
from srs.ports.inbound.accession import AccessionPort
from srs.ports.outbound.dao import AccessionDao, AltAccessionDao

log = logging.getLogger(__name__)


class AccessionController(AccessionPort):
    """Core implementation of accession lookups."""

    def __init__(
        self,
        *,
        accession_dao: AccessionDao,
        alt_accession_dao: AltAccessionDao,
    ):
        self._accession_dao = accession_dao
        self._alt_accession_dao = alt_accession_dao

    async def get_accession(self, *, accession_id: str) -> Accession:
        """Get a primary accession by ID."""
        try:
            return await self._accession_dao.get_by_id(accession_id)
        except ResourceNotFoundError as err:
            raise self.AccessionNotFoundError(
                accession_id=accession_id
            ) from err

    async def get_alt_accession(
        self,
        *,
        accession_id: str,
        alt_type: AltAccessionType,
    ) -> AltAccession:
        """Get an alternative accession by ID and type."""
        async for alt in self._alt_accession_dao.find_all(
            mapping={"id": accession_id, "type": alt_type.value}
        ):
            return alt
        raise self.AccessionNotFoundError(accession_id=accession_id)
