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

"""Core implementation of Publication operations."""

import logging
from typing import Any
from uuid import UUID

from ghga_service_commons.utils.utc_dates import now_as_utc
from hexkit.protocols.dao import ResourceNotFoundError

from srs.core.accessions import generate_accession
from srs.core.models import (
    Accession,
    AccessionType,
    Publication,
    Study,
    StudyStatus,
)
from srs.ports.inbound.publication import PublicationPort
from srs.ports.outbound.dao import AccessionDao, PublicationDao, StudyDao

log = logging.getLogger(__name__)


class PublicationController(PublicationPort):
    """Core implementation of Publication operations."""

    def __init__(
        self,
        *,
        study_dao: StudyDao,
        publication_dao: PublicationDao,
        accession_dao: AccessionDao,
    ):
        self._study_dao = study_dao
        self._publication_dao = publication_dao
        self._accession_dao = accession_dao

    # --- Helpers ---

    async def _get_study_or_raise(self, study_id: str) -> Study:
        """Retrieve a study by ID or raise StudyNotFoundError."""
        try:
            return await self._study_dao.get_by_id(study_id)
        except ResourceNotFoundError as err:
            raise self.StudyNotFoundError(study_id=study_id) from err

    async def _require_pending(self, study: Study) -> None:
        """Raise StatusConflictError if the study is not PENDING."""
        if study.status != StudyStatus.PENDING:
            raise self.StatusConflictError(
                detail=f"Study {study.id} has status {study.status}; "
                "expected PENDING."
            )

    def _check_user_access(
        self,
        study: Study,
        user_id: UUID | None,
        is_data_steward: bool,
    ) -> None:
        """Check if a user has access to a study."""
        if is_data_steward:
            return
        if study.users is None:
            return
        if user_id is not None and user_id in study.users:
            return
        raise self.AccessDeniedError()

    # --- Publication operations ---

    async def create_publication(
        self,
        *,
        data: dict[str, Any],
    ) -> Publication:
        """Create a publication for a study."""
        study_id = data["study_id"]
        study = await self._get_study_or_raise(study_id)
        await self._require_pending(study)

        pub_accession = generate_accession(AccessionType.PUBLICATION)
        today = now_as_utc()

        # Register the accession
        accession = Accession(
            id=pub_accession,
            type=AccessionType.PUBLICATION,
            created=today,
        )
        await self._accession_dao.insert(accession)

        publication = Publication(
            id=pub_accession,
            title=data["title"],
            abstract=data.get("abstract"),
            authors=data.get("authors", []),
            year=data["year"],
            journal=data.get("journal"),
            doi=data.get("doi"),
            study_id=study_id,
            created=today,
        )
        await self._publication_dao.insert(publication)
        log.info("Created publication %s for study %s", pub_accession, study_id)
        return publication

    async def get_publications(
        self,
        *,
        year: int | None = None,
        text: str | None = None,
        skip: int = 0,
        limit: int = 100,
        user_id: UUID | None = None,
        is_data_steward: bool = False,
    ) -> list[Publication]:
        """Get publications filtered by optional parameters."""
        mapping: dict = {}
        if year is not None:
            mapping["year"] = year

        publications: list[Publication] = []
        async for pub in self._publication_dao.find_all(mapping=mapping):
            # Check study access
            try:
                study = await self._study_dao.get_by_id(pub.study_id)
            except ResourceNotFoundError:
                continue
            if not is_data_steward:
                if study.users is not None:
                    if user_id is None or user_id not in study.users:
                        continue

            # Text filter
            if text is not None:
                text_lower = text.lower()
                fields = [pub.title] + pub.authors
                if pub.abstract:
                    fields.append(pub.abstract)
                if pub.journal:
                    fields.append(pub.journal)
                if not any(text_lower in f.lower() for f in fields):
                    continue

            publications.append(pub)

        return publications[skip : skip + limit]

    async def get_publication(
        self,
        *,
        publication_id: str,
        user_id: UUID | None = None,
        is_data_steward: bool = False,
    ) -> Publication:
        """Get a publication by its PID."""
        try:
            pub = await self._publication_dao.get_by_id(publication_id)
        except ResourceNotFoundError as err:
            raise self.PublicationNotFoundError(
                publication_id=publication_id
            ) from err

        study = await self._get_study_or_raise(pub.study_id)
        self._check_user_access(study, user_id, is_data_steward)
        return pub

    async def delete_publication(self, *, publication_id: str) -> None:
        """Delete a publication and its accession."""
        try:
            pub = await self._publication_dao.get_by_id(publication_id)
        except ResourceNotFoundError as err:
            raise self.PublicationNotFoundError(
                publication_id=publication_id
            ) from err

        study = await self._get_study_or_raise(pub.study_id)
        await self._require_pending(study)

        try:
            await self._accession_dao.delete(publication_id)
        except ResourceNotFoundError:
            pass
        await self._publication_dao.delete(publication_id)
        log.info("Deleted publication %s", publication_id)
