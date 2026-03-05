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

"""Core implementation of Dataset operations."""

import logging
from typing import Any

from ghga_service_commons.utils.utc_dates import now_as_utc
from hexkit.protocols.dao import ResourceNotFoundError
from uuid import UUID

from srs.core.accessions import generate_accession
from srs.core.utils import check_user_access, get_study_or_raise, require_pending
from srs.core.models import (
    Accession,
    AccessionType,
    Dataset,
)
from srs.ports.inbound.data_access import DataAccessPort
from srs.ports.inbound.dataset import DatasetPort
from srs.ports.outbound.dao import (
    AccessionDao,
    DatasetDao,
    StudyDao,
)

log = logging.getLogger(__name__)


class DatasetController(DatasetPort):
    """Core implementation of Dataset operations."""

    def __init__(
        self,
        *,
        dataset_dao: DatasetDao,
        study_dao: StudyDao,
        accession_dao: AccessionDao,
        data_access: DataAccessPort,
    ):
        self._dataset_dao = dataset_dao
        self._study_dao = study_dao
        self._accession_dao = accession_dao
        self._data_access = data_access

    # --- Dataset operations ---

    async def create_dataset(
        self,
        *,
        data: dict[str, Any],
    ) -> Dataset:
        """Create or update a dataset for a study."""
        study_id = data["study_id"]
        dap_id = data["dap_id"]
        files = data.get("files", [])

        study = await get_study_or_raise(self._study_dao, study_id)
        await require_pending(study)

        # Verify DAP exists
        try:
            await self._data_access.get_dap(dap_id=dap_id)
        except DataAccessPort.DapNotFoundError as err:
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
        today = now_as_utc()

        accession = Accession(
            id=dataset_accession,
            type=AccessionType.DATASET,
            created=today,
        )
        await self._accession_dao.insert(accession)

        dataset = Dataset(
            **data,
            id=dataset_accession,
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

        study = await get_study_or_raise(self._study_dao, dataset.study_id)
        check_user_access(study, user_id, is_data_steward)
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
            await self._data_access.get_dap(dap_id=dap_id)
        except DataAccessPort.DapNotFoundError as err:
            raise self.DapNotFoundError(dap_id=dap_id) from err

        dataset = dataset.model_copy(
            update={"dap_id": dap_id, "changed": now_as_utc()}
        )
        await self._dataset_dao.update(dataset)
        log.info("Updated dataset %s DAP to %s", dataset_id, dap_id)

    async def delete_dataset(self, *, dataset_id: str) -> None:
        """Delete a dataset and its accession."""
        try:
            dataset = await self._dataset_dao.get_by_id(dataset_id)
        except ResourceNotFoundError as err:
            raise self.DatasetNotFoundError(dataset_id=dataset_id) from err

        study = await get_study_or_raise(self._study_dao, dataset.study_id)
        await require_pending(study)

        try:
            await self._accession_dao.delete(dataset_id)
        except ResourceNotFoundError:
            pass
        await self._dataset_dao.delete(dataset_id)
        log.info("Deleted dataset %s", dataset_id)
