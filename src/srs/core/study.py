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

"""Core implementation of Study CRUD and Publish operations."""

import logging
from typing import Any
from uuid import UUID

from ghga_service_commons.utils.utc_dates import UTCDatetime, now_as_utc
from hexkit.protocols.dao import ResourceNotFoundError

from srs.core.accessions import (
    EM_RESOURCE_TO_ACCESSION_TYPE,
    generate_accession,
)
from srs.core.utils import check_user_access, get_study_or_raise, require_pending
from srs.core.models import (
    Accession,
    AccessionType,
    AnnotatedExperimentalMetadata,
    DataAccessPolicyNested,
    DatasetWithDap,
    EmAccessionMap,
    PublicationNested,
    Study,
    StudyStatus,
    StudyWithPublication,
)
from srs.ports.inbound.data_access import DataAccessPort
from srs.ports.inbound.study import StudyPort
from srs.ports.outbound.dao import (
    AccessionDao,
    DatasetDao,
    EmAccessionMapDao,
    ExperimentalMetadataDao,
    PublicationDao,
    StudyDao,
)
from srs.ports.outbound.event_pub import EventPublisherPort

log = logging.getLogger(__name__)


class StudyController(StudyPort):
    """Core implementation of Study CRUD and Publish operations."""

    def __init__(
        self,
        *,
        study_dao: StudyDao,
        metadata_dao: ExperimentalMetadataDao,
        publication_dao: PublicationDao,
        dataset_dao: DatasetDao,
        accession_dao: AccessionDao,
        em_accession_map_dao: EmAccessionMapDao,
        event_publisher: EventPublisherPort,
        data_access: DataAccessPort,
    ):
        self._study_dao = study_dao
        self._metadata_dao = metadata_dao
        self._publication_dao = publication_dao
        self._dataset_dao = dataset_dao
        self._accession_dao = accession_dao
        self._em_accession_map_dao = em_accession_map_dao
        self._event_publisher = event_publisher
        self._data_access = data_access

    # --- Helpers ---

    def _strip_user_fields(
        self, study: Study, is_data_steward: bool
    ) -> Study:
        """Remove user-related fields if the caller is not a data steward."""
        if is_data_steward:
            return study
        return study.model_copy(
            update={"users": None, "created_by": UUID(int=0), "approved_by": None}
        )

    async def _validate_study_completeness(self, study_id: str) -> None:
        """Validate that a study has all required data for persisting/publishing."""
        # Must have experimental metadata
        try:
            await self._metadata_dao.get_by_id(study_id)
        except ResourceNotFoundError as err:
            raise self.ValidationError(
                detail=f"Study {study_id} has no experimental metadata."
            ) from err

        # Must have at least one publication
        publications = []
        async for pub in self._publication_dao.find_all(
            mapping={"study_id": study_id}
        ):
            publications.append(pub)
        if not publications:
            raise self.ValidationError(
                detail=f"Study {study_id} has no publication."
            )

    async def _generate_em_accessions(
        self, study_id: str
    ) -> dict[str, dict[str, str]]:
        """Generate accession numbers for all EM resources and store them."""
        em = await self._metadata_dao.get_by_id(study_id)
        metadata = em.metadata
        now = now_as_utc()
        accession_maps: dict[str, dict[str, str]] = {}

        for resource_name, accession_type in EM_RESOURCE_TO_ACCESSION_TYPE.items():
            if resource_name not in metadata:
                continue
            resource_data = metadata[resource_name]
            if not isinstance(resource_data, dict):
                continue
            resource_map: dict[str, str] = {}
            for alias in resource_data:
                accession_id = generate_accession(accession_type)
                accession = Accession(
                    id=accession_id,
                    type=accession_type,
                    created=now,
                    superseded_by=None,
                )
                await self._accession_dao.insert(accession)
                resource_map[alias] = accession_id
            if resource_map:
                accession_maps[resource_name] = resource_map

        # Store the accession map
        em_map = EmAccessionMap(id=study_id, maps=accession_maps)
        try:
            await self._em_accession_map_dao.update(em_map)
        except ResourceNotFoundError:
            await self._em_accession_map_dao.insert(em_map)

        return accession_maps

    async def _build_annotated_em(
        self, study_id: str
    ) -> AnnotatedExperimentalMetadata:
        """Build an AnnotatedExperimentalMetadata payload for event publishing."""
        study = await get_study_or_raise(self._study_dao, study_id)
        em = await self._metadata_dao.get_by_id(study_id)

        # Get accession maps
        try:
            em_map = await self._em_accession_map_dao.get_by_id(study_id)
            accessions = em_map.maps
        except ResourceNotFoundError:
            accessions = {}

        # Get publication
        publication_nested = None
        async for pub in self._publication_dao.find_all(
            mapping={"study_id": study_id}
        ):
            publication_nested = PublicationNested(
                id=pub.id,
                title=pub.title,
                abstract=pub.abstract,
                authors=pub.authors,
                year=pub.year,
                journal=pub.journal,
                doi=pub.doi,
            )
            break

        study_with_pub = StudyWithPublication(
            id=study.id,
            title=study.title,
            description=study.description,
            types=study.types,
            affiliations=study.affiliations,
            status=study.status,
            publication=publication_nested,
        )

        # Get datasets with nested DAP and DAC
        datasets_with_dap: list[DatasetWithDap] = []
        async for dataset in self._dataset_dao.find_all(
            mapping={"study_id": study_id}
        ):
            try:
                dap = await self._data_access.get_dap(dap_id=dataset.dap_id)
                dac = await self._data_access.get_dac(dac_id=dap.dac_id)
            except (
                DataAccessPort.DapNotFoundError,
                DataAccessPort.DacNotFoundError,
            ):
                continue
            dap_nested = DataAccessPolicyNested(
                id=dap.id,
                name=dap.name,
                description=dap.description,
                text=dap.text,
                url=dap.url,
                duo_permission_id=dap.duo_permission_id,
                duo_modifier_ids=dap.duo_modifier_ids,
                dac=dac,
            )
            datasets_with_dap.append(
                DatasetWithDap(
                    id=dataset.id,
                    title=dataset.title,
                    description=dataset.description,
                    types=dataset.types,
                    files=dataset.files,
                    dap=dap_nested,
                )
            )

        return AnnotatedExperimentalMetadata(
            metadata=em.metadata,
            accessions=accessions,
            study=study_with_pub,
            datasets=datasets_with_dap,
        )

    # --- Study operations ---

    async def create_study(
        self,
        *,
        data: dict[str, Any],
    ) -> Study:
        """Create a new study with status PENDING."""
        study_accession = generate_accession(AccessionType.STUDY)
        now = now_as_utc()
        created_by = data["created_by"]

        # Register the accession
        accession = Accession(
            id=study_accession,
            type=AccessionType.STUDY,
            created=now,
        )
        await self._accession_dao.insert(accession)

        study = Study(
            **data,
            id=study_accession,
            users=[created_by],
            created=now,
        )
        await self._study_dao.insert(study)
        log.info("Created study %s", study.id)
        return study

    async def get_studies(
        self,
        *,
        status: StudyStatus | None = None,
        study_type: str | None = None,
        text: str | None = None,
        skip: int = 0,
        limit: int = 100,
        user_id: UUID | None = None,
        is_data_steward: bool = False,
    ) -> list[Study]:
        """Get studies filtered by optional parameters."""
        mapping: dict = {}
        if status is not None:
            mapping["status"] = status.value

        studies: list[Study] = []
        async for study in self._study_dao.find_all(mapping=mapping):
            # Authorization filter
            if not is_data_steward:
                if study.users is not None:
                    if user_id is None or user_id not in study.users:
                        continue

            # Type filter
            if study_type is not None and study_type not in study.types:
                continue

            # Text filter (partial match in title, description, affiliations)
            if text is not None:
                text_lower = text.lower()
                if not any(
                    text_lower in field.lower()
                    for field in [study.title, study.description]
                    + study.affiliations
                ):
                    continue

            studies.append(self._strip_user_fields(study, is_data_steward))

        # Pagination
        return studies[skip : skip + limit]

    async def get_study(
        self,
        *,
        study_id: str,
        user_id: UUID | None = None,
        is_data_steward: bool = False,
    ) -> Study:
        """Get a study by its PID."""
        study = await get_study_or_raise(self._study_dao, study_id)
        check_user_access(study, user_id, is_data_steward)
        return self._strip_user_fields(study, is_data_steward)

    async def update_study(
        self,
        *,
        study_id: str,
        status: StudyStatus | None = None,
        users: list[UUID] | None = None,
        approved_by: UUID | None = None,
    ) -> None:
        """Update study status and/or users."""
        study = await get_study_or_raise(self._study_dao, study_id)

        if status is not None:
            # Only PENDING -> PERSISTED is allowed
            if not (
                study.status == StudyStatus.PENDING
                and status == StudyStatus.PERSISTED
            ):
                raise self.StatusConflictError(
                    detail=f"Cannot change status from {study.status} to {status}."
                )
            # Validate before status change
            await self._validate_study_completeness(study_id)
            study = study.model_copy(
                update={
                    "status": status,
                    "approved_by": approved_by,
                }
            )

        if users is not None:
            # users can only be set to None when status is PERSISTED
            study = study.model_copy(update={"users": users})
        elif status == StudyStatus.PERSISTED:
            # Allow explicitly unsetting users via None in the request
            pass

        await self._study_dao.update(study)
        log.info("Updated study %s", study_id)

    async def delete_study(self, *, study_id: str) -> None:
        """Delete a study and all related entities."""
        study = await get_study_or_raise(self._study_dao, study_id)
        await require_pending(study)

        # Delete related experimental metadata
        try:
            await self._metadata_dao.delete(study_id)
        except ResourceNotFoundError:
            pass

        # Delete related publications
        async for pub in self._publication_dao.find_all(
            mapping={"study_id": study_id}
        ):
            try:
                await self._accession_dao.delete(pub.id)
            except ResourceNotFoundError:
                pass
            await self._publication_dao.delete(pub.id)

        # Delete related datasets
        async for dataset in self._dataset_dao.find_all(
            mapping={"study_id": study_id}
        ):
            try:
                await self._accession_dao.delete(dataset.id)
            except ResourceNotFoundError:
                pass
            await self._dataset_dao.delete(dataset.id)

        # Delete accession maps
        try:
            em_map = await self._em_accession_map_dao.get_by_id(study_id)
            # Delete all EM accessions
            for resource_map in em_map.maps.values():
                for accession_id in resource_map.values():
                    try:
                        await self._accession_dao.delete(accession_id)
                    except ResourceNotFoundError:
                        pass
            await self._em_accession_map_dao.delete(study_id)
        except ResourceNotFoundError:
            pass

        # Delete study accession
        try:
            await self._accession_dao.delete(study_id)
        except ResourceNotFoundError:
            pass

        await self._study_dao.delete(study_id)
        log.info("Deleted study %s and all related entities", study_id)

    # --- Publish operations ---

    async def publish_study(self, *, study_id: str) -> None:
        """Validate and publish a study."""
        await get_study_or_raise(self._study_dao, study_id)
        await self._validate_study_completeness(study_id)

        # Generate accessions for EM resources
        await self._generate_em_accessions(study_id)

        # Build and publish annotated EM
        aem = await self._build_annotated_em(study_id)
        await self._event_publisher.publish_annotated_metadata(payload=aem)
        log.info("Published study %s", study_id)
