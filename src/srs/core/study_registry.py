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

"""Core implementation of the Study Registry Service."""

import logging
from ghga_service_commons.utils.utc_dates import UTCDatetime, now_as_utc
from uuid import UUID, uuid4

from hexkit.protocols.dao import ResourceNotFoundError

from srs.core.accessions import (
    EM_RESOURCE_TO_ACCESSION_TYPE,
    generate_accession,
)
from srs.core.models import (
    Accession,
    AccessionType,
    AltAccession,
    AltAccessionType,
    AnnotatedExperimentalMetadata,
    DataAccessCommittee,
    DataAccessPolicy,
    DataAccessPolicyNested,
    Dataset,
    DatasetWithDap,
    DuoModifier,
    DuoPermission,
    EmAccessionMap,
    ExperimentalMetadata,
    Publication,
    PublicationNested,
    ResourceType,
    Study,
    StudyStatus,
    StudyWithPublication,
    TypedResource,
)
from srs.ports.inbound.study_registry import StudyRegistryPort
from srs.ports.outbound.dao import (
    AccessionDao,
    AltAccessionDao,
    DataAccessCommitteeDao,
    DataAccessPolicyDao,
    DatasetDao,
    EmAccessionMapDao,
    ExperimentalMetadataDao,
    PublicationDao,
    ResourceTypeDao,
    StudyDao,
)
from srs.ports.outbound.event_pub import EventPublisherPort

log = logging.getLogger(__name__)


def _now() -> UTCDatetime:
    """Return the current UTC datetime."""
    return now_as_utc()


class StudyRegistryController(StudyRegistryPort):
    """Core implementation of all Study Registry operations."""

    def __init__(
        self,
        *,
        study_dao: StudyDao,
        metadata_dao: ExperimentalMetadataDao,
        publication_dao: PublicationDao,
        dac_dao: DataAccessCommitteeDao,
        dap_dao: DataAccessPolicyDao,
        dataset_dao: DatasetDao,
        resource_type_dao: ResourceTypeDao,
        accession_dao: AccessionDao,
        alt_accession_dao: AltAccessionDao,
        em_accession_map_dao: EmAccessionMapDao,
        event_publisher: EventPublisherPort,
    ):
        self._study_dao = study_dao
        self._metadata_dao = metadata_dao
        self._publication_dao = publication_dao
        self._dac_dao = dac_dao
        self._dap_dao = dap_dao
        self._dataset_dao = dataset_dao
        self._resource_type_dao = resource_type_dao
        self._accession_dao = accession_dao
        self._alt_accession_dao = alt_accession_dao
        self._em_accession_map_dao = em_accession_map_dao
        self._event_publisher = event_publisher

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
            return  # publicly accessible
        if user_id is not None and user_id in study.users:
            return
        raise self.AccessDeniedError()

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
        today = _now()
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
                    created=today,
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
        study = await self._get_study_or_raise(study_id)
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
                dap = await self._dap_dao.get_by_id(dataset.dap_id)
                dac = await self._dac_dao.get_by_id(dap.dac_id)
            except ResourceNotFoundError:
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
        title: str,
        description: str,
        types: list[str],
        affiliations: list[str],
        created_by: UUID,
    ) -> Study:
        """Create a new study with status PENDING."""
        study_accession = generate_accession(AccessionType.STUDY)
        today = _now()

        # Register the accession
        accession = Accession(
            id=study_accession,
            type=AccessionType.STUDY,
            created=today,
        )
        await self._accession_dao.insert(accession)

        study = Study(
            id=study_accession,
            title=title,
            description=description,
            types=types,
            affiliations=affiliations,
            status=StudyStatus.PENDING,
            users=[created_by],
            created=today,
            created_by=created_by,
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
        study = await self._get_study_or_raise(study_id)
        self._check_user_access(study, user_id, is_data_steward)
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
        study = await self._get_study_or_raise(study_id)

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
        study = await self._get_study_or_raise(study_id)
        await self._require_pending(study)

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

    # --- ExperimentalMetadata operations ---

    async def upsert_metadata(
        self, *, study_id: str, metadata: dict
    ) -> None:
        """Create or update experimental metadata for a study."""
        study = await self._get_study_or_raise(study_id)
        await self._require_pending(study)

        today = _now()
        em = ExperimentalMetadata(
            id=study_id,
            metadata=metadata,
            submitted=today,
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
        study = await self._get_study_or_raise(study_id)
        await self._require_pending(study)
        try:
            await self._metadata_dao.delete(study_id)
        except ResourceNotFoundError as err:
            raise self.MetadataNotFoundError(study_id=study_id) from err
        log.info("Deleted experimental metadata for study %s", study_id)

    # --- Publication operations ---

    async def create_publication(
        self,
        *,
        title: str,
        abstract: str | None,
        authors: list[str],
        year: int,
        journal: str | None,
        doi: str | None,
        study_id: str,
    ) -> Publication:
        """Create or update a publication for a study."""
        study = await self._get_study_or_raise(study_id)
        await self._require_pending(study)

        pub_accession = generate_accession(AccessionType.PUBLICATION)
        today = _now()

        # Register the accession
        accession = Accession(
            id=pub_accession,
            type=AccessionType.PUBLICATION,
            created=today,
        )
        await self._accession_dao.insert(accession)

        publication = Publication(
            id=pub_accession,
            title=title,
            abstract=abstract,
            authors=authors,
            year=year,
            journal=journal,
            doi=doi,
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

    # --- DataAccessCommittee operations ---

    async def create_dac(
        self,
        *,
        id: str,
        name: str,
        email: str,
        institute: str,
    ) -> None:
        """Create a new DAC."""
        today = _now()
        dac = DataAccessCommittee(
            id=id,
            name=name,
            email=email,
            institute=institute,
            created=today,
            changed=today,
            active=True,
        )
        try:
            await self._dac_dao.insert(dac)
        except Exception as err:
            raise self.DuplicateError(
                detail=f"DAC with ID {id} already exists."
            ) from err
        log.info("Created DAC %s", id)

    async def get_dacs(self) -> list[DataAccessCommittee]:
        """Get all DACs."""
        return [dac async for dac in self._dac_dao.find_all(mapping={})]

    async def get_dac(self, *, dac_id: str) -> DataAccessCommittee:
        """Get a DAC by ID."""
        try:
            return await self._dac_dao.get_by_id(dac_id)
        except ResourceNotFoundError as err:
            raise self.DacNotFoundError(dac_id=dac_id) from err

    async def update_dac(
        self,
        *,
        dac_id: str,
        name: str | None = None,
        email: str | None = None,
        institute: str | None = None,
        active: bool | None = None,
    ) -> None:
        """Update a DAC."""
        try:
            dac = await self._dac_dao.get_by_id(dac_id)
        except ResourceNotFoundError as err:
            raise self.DacNotFoundError(dac_id=dac_id) from err

        updates: dict = {"changed": _now()}
        if name is not None:
            updates["name"] = name
        if email is not None:
            updates["email"] = email
        if institute is not None:
            updates["institute"] = institute
        if active is not None:
            updates["active"] = active

        dac = dac.model_copy(update=updates)
        await self._dac_dao.update(dac)
        log.info("Updated DAC %s", dac_id)

    async def delete_dac(self, *, dac_id: str) -> None:
        """Delete a DAC."""
        try:
            await self._dac_dao.get_by_id(dac_id)
        except ResourceNotFoundError as err:
            raise self.DacNotFoundError(dac_id=dac_id) from err

        # Check for referencing DAPs
        async for dap in self._dap_dao.find_all(mapping={"dac_id": dac_id}):
            raise self.ReferenceConflictError(
                detail=f"Cannot delete DAC {dac_id}; "
                f"it is referenced by DAP {dap.id}."
            )

        await self._dac_dao.delete(dac_id)
        log.info("Deleted DAC %s", dac_id)

    # --- DataAccessPolicy operations ---

    async def create_dap(
        self,
        *,
        id: str,
        name: str,
        description: str,
        text: str,
        url: str | None,
        duo_permission_id: str,
        duo_modifier_ids: list[str],
        dac_id: str,
    ) -> None:
        """Create a new DAP."""
        # Verify DAC exists
        try:
            await self._dac_dao.get_by_id(dac_id)
        except ResourceNotFoundError as err:
            raise self.DacNotFoundError(dac_id=dac_id) from err

        today = _now()
        dap = DataAccessPolicy(
            id=id,
            name=name,
            description=description,
            text=text,
            url=url,
            duo_permission_id=DuoPermission(duo_permission_id),
            duo_modifier_ids=[DuoModifier(m) for m in duo_modifier_ids],
            dac_id=dac_id,
            created=today,
            changed=today,
            active=True,
        )
        try:
            await self._dap_dao.insert(dap)
        except Exception as err:
            raise self.DuplicateError(
                detail=f"DAP with ID {id} already exists."
            ) from err
        log.info("Created DAP %s", id)

    async def get_daps(self) -> list[DataAccessPolicy]:
        """Get all DAPs."""
        return [dap async for dap in self._dap_dao.find_all(mapping={})]

    async def get_dap(self, *, dap_id: str) -> DataAccessPolicy:
        """Get a DAP by ID."""
        try:
            return await self._dap_dao.get_by_id(dap_id)
        except ResourceNotFoundError as err:
            raise self.DapNotFoundError(dap_id=dap_id) from err

    async def update_dap(
        self,
        *,
        dap_id: str,
        name: str | None = None,
        description: str | None = None,
        text: str | None = None,
        url: str | None = None,
        duo_permission_id: str | None = None,
        duo_modifier_ids: list[str] | None = None,
        dac_id: str | None = None,
        active: bool | None = None,
    ) -> None:
        """Update a DAP."""
        try:
            dap = await self._dap_dao.get_by_id(dap_id)
        except ResourceNotFoundError as err:
            raise self.DapNotFoundError(dap_id=dap_id) from err

        if dac_id is not None:
            try:
                await self._dac_dao.get_by_id(dac_id)
            except ResourceNotFoundError as err:
                raise self.DacNotFoundError(dac_id=dac_id) from err

        updates: dict = {"changed": _now()}
        if name is not None:
            updates["name"] = name
        if description is not None:
            updates["description"] = description
        if text is not None:
            updates["text"] = text
        if url is not None:
            updates["url"] = url
        if duo_permission_id is not None:
            updates["duo_permission_id"] = DuoPermission(duo_permission_id)
        if duo_modifier_ids is not None:
            updates["duo_modifier_ids"] = [
                DuoModifier(m) for m in duo_modifier_ids
            ]
        if dac_id is not None:
            updates["dac_id"] = dac_id
        if active is not None:
            updates["active"] = active

        dap = dap.model_copy(update=updates)
        await self._dap_dao.update(dap)
        log.info("Updated DAP %s", dap_id)

    async def delete_dap(self, *, dap_id: str) -> None:
        """Delete a DAP."""
        try:
            await self._dap_dao.get_by_id(dap_id)
        except ResourceNotFoundError as err:
            raise self.DapNotFoundError(dap_id=dap_id) from err

        # Check for referencing datasets
        async for ds in self._dataset_dao.find_all(mapping={"dap_id": dap_id}):
            raise self.ReferenceConflictError(
                detail=f"Cannot delete DAP {dap_id}; "
                f"it is referenced by dataset {ds.id}."
            )

        await self._dap_dao.delete(dap_id)
        log.info("Deleted DAP %s", dap_id)

    # --- Dataset operations ---

    async def create_dataset(
        self,
        *,
        title: str,
        description: str,
        types: list[str],
        study_id: str,
        dap_id: str,
        files: list[str],
    ) -> Dataset:
        """Create or update a dataset for a study."""
        study = await self._get_study_or_raise(study_id)
        await self._require_pending(study)

        # Verify DAP exists
        try:
            await self._dap_dao.get_by_id(dap_id)
        except ResourceNotFoundError as err:
            raise self.DapNotFoundError(dap_id=dap_id) from err

        # Validate files exist in EM and are unique
        if files:
            seen: set[str] = set()
            for f in files:
                if f in seen:
                    raise self.ValidationError(
                        detail=f"Duplicate file alias: {f}"
                    )
                seen.add(f)

        dataset_accession = generate_accession(AccessionType.DATASET)
        today = _now()

        accession = Accession(
            id=dataset_accession,
            type=AccessionType.DATASET,
            created=today,
        )
        await self._accession_dao.insert(accession)

        dataset = Dataset(
            id=dataset_accession,
            title=title,
            description=description,
            types=types,
            study_id=study_id,
            dap_id=dap_id,
            files=files,
            created=today,
            changed=today,
        )
        await self._dataset_dao.insert(dataset)
        log.info(
            "Created dataset %s for study %s", dataset_accession, study_id
        )
        return dataset

    async def get_datasets(
        self,
        *,
        dataset_type: str | None = None,
        study_id: str | None = None,
        text: str | None = None,
        skip: int = 0,
        limit: int = 100,
        user_id: UUID | None = None,
        is_data_steward: bool = False,
    ) -> list[Dataset]:
        """Get datasets filtered by optional parameters."""
        mapping: dict = {}
        if study_id is not None:
            mapping["study_id"] = study_id

        datasets: list[Dataset] = []
        async for dataset in self._dataset_dao.find_all(mapping=mapping):
            # Check study access
            try:
                study = await self._study_dao.get_by_id(dataset.study_id)
            except ResourceNotFoundError:
                continue
            if not is_data_steward:
                if study.users is not None:
                    if user_id is None or user_id not in study.users:
                        continue

            # Type filter
            if dataset_type is not None and dataset_type not in dataset.types:
                continue

            # Text filter
            if text is not None:
                text_lower = text.lower()
                if not any(
                    text_lower in field.lower()
                    for field in [dataset.title, dataset.description]
                ):
                    continue

            datasets.append(dataset)

        return datasets[skip : skip + limit]

    async def get_dataset(
        self,
        *,
        dataset_id: str,
        user_id: UUID | None = None,
        is_data_steward: bool = False,
    ) -> Dataset:
        """Get a dataset by its PID."""
        try:
            dataset = await self._dataset_dao.get_by_id(dataset_id)
        except ResourceNotFoundError as err:
            raise self.DatasetNotFoundError(dataset_id=dataset_id) from err

        study = await self._get_study_or_raise(dataset.study_id)
        self._check_user_access(study, user_id, is_data_steward)
        return dataset

    async def update_dataset(
        self, *, dataset_id: str, dap_id: str
    ) -> None:
        """Update the DAP assignment for a dataset."""
        try:
            dataset = await self._dataset_dao.get_by_id(dataset_id)
        except ResourceNotFoundError as err:
            raise self.DatasetNotFoundError(dataset_id=dataset_id) from err

        # Verify DAP exists
        try:
            await self._dap_dao.get_by_id(dap_id)
        except ResourceNotFoundError as err:
            raise self.DapNotFoundError(dap_id=dap_id) from err

        dataset = dataset.model_copy(
            update={"dap_id": dap_id, "changed": _now()}
        )
        await self._dataset_dao.update(dataset)
        log.info("Updated dataset %s DAP to %s", dataset_id, dap_id)

    async def delete_dataset(self, *, dataset_id: str) -> None:
        """Delete a dataset and its accession."""
        try:
            dataset = await self._dataset_dao.get_by_id(dataset_id)
        except ResourceNotFoundError as err:
            raise self.DatasetNotFoundError(dataset_id=dataset_id) from err

        study = await self._get_study_or_raise(dataset.study_id)
        await self._require_pending(study)

        try:
            await self._accession_dao.delete(dataset_id)
        except ResourceNotFoundError:
            pass
        await self._dataset_dao.delete(dataset_id)
        log.info("Deleted dataset %s", dataset_id)

    # --- ResourceType operations ---

    async def create_resource_type(
        self,
        *,
        code: str,
        resource: TypedResource,
        name: str,
        description: str | None,
    ) -> ResourceType:
        """Create a new resource type."""
        today = _now()
        rt = ResourceType(
            id=uuid4(),
            code=code.upper(),
            resource=resource,
            name=name,
            description=description,
            created=today,
            changed=today,
            active=True,
        )
        await self._resource_type_dao.insert(rt)
        log.info("Created resource type %s (%s)", rt.code, rt.resource)
        return rt

    async def get_resource_types(
        self,
        *,
        resource: TypedResource | None = None,
        text: str | None = None,
        skip: int = 0,
        limit: int = 100,
    ) -> list[ResourceType]:
        """Get resource types filtered by optional parameters."""
        mapping: dict = {}
        if resource is not None:
            mapping["resource"] = resource.value

        types: list[ResourceType] = []
        async for rt in self._resource_type_dao.find_all(mapping=mapping):
            if text is not None:
                text_lower = text.lower()
                fields = [rt.name, rt.code]
                if rt.description:
                    fields.append(rt.description)
                if not any(text_lower in f.lower() for f in fields):
                    continue
            types.append(rt)

        return types[skip : skip + limit]

    async def get_resource_type(
        self, *, resource_type_id: UUID
    ) -> ResourceType:
        """Get a resource type by internal ID."""
        try:
            return await self._resource_type_dao.get_by_id(
                str(resource_type_id)
            )
        except ResourceNotFoundError as err:
            raise self.ResourceTypeNotFoundError(
                resource_type_id=resource_type_id
            ) from err

    async def update_resource_type(
        self,
        *,
        resource_type_id: UUID,
        name: str | None = None,
        description: str | None = None,
        active: bool | None = None,
    ) -> None:
        """Update a resource type."""
        try:
            rt = await self._resource_type_dao.get_by_id(
                str(resource_type_id)
            )
        except ResourceNotFoundError as err:
            raise self.ResourceTypeNotFoundError(
                resource_type_id=resource_type_id
            ) from err

        updates: dict = {"changed": _now()}
        if name is not None:
            updates["name"] = name
        if description is not None:
            updates["description"] = description
        if active is not None:
            updates["active"] = active

        rt = rt.model_copy(update=updates)
        await self._resource_type_dao.update(rt)
        log.info("Updated resource type %s", resource_type_id)

    async def delete_resource_type(
        self, *, resource_type_id: UUID
    ) -> None:
        """Delete a resource type."""
        try:
            rt = await self._resource_type_dao.get_by_id(
                str(resource_type_id)
            )
        except ResourceNotFoundError as err:
            raise self.ResourceTypeNotFoundError(
                resource_type_id=resource_type_id
            ) from err

        # Check if still referenced by any study or dataset
        mapping_field = (
            "types" if rt.resource in (TypedResource.STUDY, TypedResource.DATASET) else None
        )
        if mapping_field:
            target_dao = (
                self._study_dao
                if rt.resource == TypedResource.STUDY
                else self._dataset_dao
            )
            async for entity in target_dao.find_all(mapping={}):
                if rt.code in entity.types:
                    raise self.ReferenceConflictError(
                        detail=f"Cannot delete resource type {rt.code}; "
                        f"it is still referenced by {entity.id}."
                    )

        await self._resource_type_dao.delete(str(resource_type_id))
        log.info("Deleted resource type %s", resource_type_id)

    # --- Accession operations ---

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

    # --- Filename operations ---

    async def get_filenames(
        self, *, study_id: str
    ) -> dict[str, dict[str, str]]:
        """Get file accession to filename/alias mapping for a study."""
        await self._get_study_or_raise(study_id)

        # Get the EM accession map for files
        try:
            em_map = await self._em_accession_map_dao.get_by_id(study_id)
        except ResourceNotFoundError as err:
            raise self.MetadataNotFoundError(study_id=study_id) from err

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
        await self._get_study_or_raise(study_id)

        today = _now()
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
                created=today,
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

    # --- Publish operations ---

    async def publish_study(self, *, study_id: str) -> None:
        """Validate and publish a study."""
        await self._get_study_or_raise(study_id)
        await self._validate_study_completeness(study_id)

        # Generate accessions for EM resources
        await self._generate_em_accessions(study_id)

        # Build and publish annotated EM
        aem = await self._build_annotated_em(study_id)
        await self._event_publisher.publish_annotated_metadata(payload=aem)
        log.info("Published study %s", study_id)
