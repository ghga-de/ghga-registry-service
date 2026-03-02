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

"""Basic smoke tests for the Study Registry Service."""

from datetime import date
from uuid import UUID

from srs.core.accessions import generate_accession
from srs.core.models import (
    AccessionType,
    DataAccessCommittee,
    Study,
    StudyStatus,
)


def test_generate_accession_format():
    """Accession IDs must follow the GHGA{letter}{14 digits} pattern."""
    accession = generate_accession(AccessionType.STUDY)
    assert accession.startswith("GHGAS")
    assert len(accession) == 19  # 4 (GHGA) + 1 (letter) + 14 (digits)


def test_study_model():
    """A Study model can be constructed and serialised."""
    study = Study(
        id="GHGAS00000000000001",
        title="Test Study",
        description="A test.",
        types=["WGS"],
        affiliations=["GHGA"],
        status=StudyStatus.PENDING,
        users=[UUID(int=1)],
        created=date(2025, 1, 1),
        created_by=UUID(int=1),
    )
    assert study.status == StudyStatus.PENDING
    data = study.model_dump()
    assert data["id"] == "GHGAS00000000000001"


def test_dac_model_with_email():
    """A DAC model validates the email field."""
    dac = DataAccessCommittee(
        id="DAC-1",
        name="My DAC",
        email="dac@example.org",
        institute="GHGA",
        created=date(2025, 1, 1),
        changed=date(2025, 1, 1),
    )
    assert dac.email == "dac@example.org"
