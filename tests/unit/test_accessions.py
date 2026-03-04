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

"""Tests for Accession user stories.

Spec: GET /accession/{id}, GET /accession/{id}?type={type}
"""

from datetime import datetime

import pytest
from ghga_service_commons.utils.utc_dates import now_as_utc

from srs.core.models import AltAccession, AltAccessionType
from srs.ports.inbound.study_registry import StudyRegistryPort
from tests.conftest import USER_SUBMITTER
from tests.fixtures.examples import EXAMPLES

E = EXAMPLES


# ── GET /accession/{id} (primary) ───────────────────────────────


@pytest.mark.asyncio
async def test_get_accession(controller, accession_dao):
    """Getting a primary accession by ID must return it."""
    # Creating a study automatically registers an accession
    study = await controller.studies.create_study(
        **E["studies"]["minimal"], created_by=USER_SUBMITTER,
    )
    acc = await controller.get_accession(accession_id=study.id)
    assert acc.id == study.id
    assert acc.type == "STUDY"
    assert isinstance(acc.created, datetime)


@pytest.mark.asyncio
async def test_get_accession_not_found(controller):
    """Getting a non-existent accession must raise AccessionNotFoundError."""
    with pytest.raises(StudyRegistryPort.AccessionNotFoundError):
        await controller.get_accession(accession_id="NONEXIST")


# ── GET /accession/{id}?type={type} (alternative) ───────────────


@pytest.mark.asyncio
async def test_get_alt_accession(controller, alt_accession_dao):
    """Getting an alt accession by ID and type must return it."""
    # Insert an alt accession directly
    alt = AltAccession(
        **E["alt_accessions"]["file_id"],
        created=now_as_utc(),
    )
    await alt_accession_dao.insert(alt)

    result = await controller.get_alt_accession(
        accession_id="FILE-001", alt_type=AltAccessionType.FILE_ID
    )
    assert result.pid == "GHGAF00000000000001"


@pytest.mark.asyncio
async def test_get_alt_accession_not_found(controller):
    """Getting a non-existent alt accession must raise AccessionNotFoundError."""
    with pytest.raises(StudyRegistryPort.AccessionNotFoundError):
        await controller.get_alt_accession(
            accession_id="NONEXIST", alt_type=AltAccessionType.FILE_ID
        )


@pytest.mark.asyncio
async def test_get_alt_accession_wrong_type(controller, alt_accession_dao):
    """Requesting a different alt type must not return existing entries."""
    alt = AltAccession(
        **E["alt_accessions"]["file_id"],
        created=now_as_utc(),
    )
    await alt_accession_dao.insert(alt)

    with pytest.raises(StudyRegistryPort.AccessionNotFoundError):
        await controller.get_alt_accession(
            accession_id="FILE-001", alt_type=AltAccessionType.EGA
        )
