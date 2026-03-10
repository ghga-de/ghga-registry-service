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

"""Tests for Filename (file-ID mapping) user stories.

Spec: GET /filenames/{study_id}, POST /filenames/{study_id}
"""

import pytest

from srs.core.models import AltAccessionType
from srs.ports.inbound.study_registry import StudyRegistryPort
from tests.conftest import USER_SUBMITTER
from tests.fixtures.examples import EXAMPLES

E = EXAMPLES


# ── GET /filenames/{study_id} ───────────────────────────────────


@pytest.mark.asyncio
async def test_get_filenames(controller, published_study_id):
    """Getting filenames must return accession→{name, alias} mappings."""
    sid = published_study_id
    result = await controller.filenames.get_filenames(study_id=sid)
    # Should have 2 file accession entries
    assert len(result) == 2
    # Each entry must have name and alias
    for acc_id, info in result.items():
        assert "name" in info
        assert "alias" in info
        assert acc_id.startswith("GHGAF")


@pytest.mark.asyncio
async def test_get_filenames_study_not_found(controller):
    """Getting filenames for a non-existent study must raise StudyNotFoundError."""
    with pytest.raises(StudyRegistryPort.StudyNotFoundError):
        await controller.filenames.get_filenames(study_id="NONEXIST")


@pytest.mark.asyncio
async def test_get_filenames_no_metadata(controller):
    """Getting filenames for a study without published EM must raise MetadataNotFoundError."""
    study = await controller.studies.create_study(
        data={**E["studies"]["minimal"], "created_by": USER_SUBMITTER},
    )
    with pytest.raises(StudyRegistryPort.MetadataNotFoundError):
        await controller.filenames.get_filenames(study_id=study.id)


# ── POST /filenames/{study_id} ──────────────────────────────────


@pytest.mark.asyncio
async def test_post_filenames_stores_alt_accessions(
    controller, alt_accession_dao, published_study_id
):
    """Posting file IDs must create AltAccession entries with type FILE_ID."""
    sid = published_study_id
    filenames = await controller.filenames.get_filenames(study_id=sid)
    file_acc_ids = list(filenames.keys())

    # Map each file accession to an internal file ID
    file_id_map = {acc: f"internal-{i}" for i, acc in enumerate(file_acc_ids)}
    await controller.filenames.post_filenames(study_id=sid, file_id_map=file_id_map)

    # Verify AltAccession entries
    for expected_id in file_id_map.values():
        alt = await alt_accession_dao.get_by_id(expected_id)
        assert alt.type == AltAccessionType.FILE_ID


@pytest.mark.asyncio
async def test_post_filenames_publishes_event(
    controller, event_store, published_study_id
):
    """Posting file IDs must publish a file-ID mapping event."""
    sid = published_study_id
    filenames = await controller.filenames.get_filenames(study_id=sid)
    file_acc_ids = list(filenames.keys())

    file_id_map = {acc: f"id-{i}" for i, acc in enumerate(file_acc_ids)}

    # The setup already published one AEM event; record baseline
    topic = event_store.topics["file_id_mapping"]
    baseline = len(topic)
    await controller.filenames.post_filenames(study_id=sid, file_id_map=file_id_map)
    assert len(topic) == baseline + 1
    assert topic[-1].payload == {"mapping": file_id_map}


@pytest.mark.asyncio
async def test_post_filenames_study_not_found(controller):
    """Posting file IDs for a non-existent study must raise StudyNotFoundError."""
    with pytest.raises(StudyRegistryPort.StudyNotFoundError):
        await controller.filenames.post_filenames(
            study_id="NONEXIST", file_id_map={"X": "Y"}
        )


@pytest.mark.asyncio
async def test_post_filenames_invalid_accession(controller, published_study_id):
    """Posting with a non-existent file accession must raise ValidationError."""
    sid = published_study_id
    with pytest.raises(StudyRegistryPort.ValidationError):
        await controller.filenames.post_filenames(
            study_id=sid,
            file_id_map={"GHGAF_INVALID_00000": "some-id"},
        )
