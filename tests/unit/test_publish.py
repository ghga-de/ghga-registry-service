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

"""Tests for the Publish user story.

Spec: POST /rpc/publish/{study_id}
"""

import pytest

from srs.ports.inbound.study_registry import StudyRegistryPort
from tests.conftest import USER_SUBMITTER
from tests.fixtures.examples import EXAMPLES

E = EXAMPLES


# ── POST /rpc/publish/{study_id} ────────────────────────────────


@pytest.mark.asyncio
async def test_publish_study_generates_em_accessions(
    controller, em_accession_map_dao, complete_study_id
):
    """Publishing must generate accessions for each EM resource."""
    sid = complete_study_id
    await controller.studies.publish_study(study_id=sid)

    em_map = await em_accession_map_dao.get_by_id(sid)
    # Should have accession maps for: files, samples, individuals
    assert "files" in em_map.maps
    assert "samples" in em_map.maps
    assert "individuals" in em_map.maps
    # Each alias should map to a GHGA accession
    assert len(em_map.maps["files"]) == 2
    for accession_id in em_map.maps["files"].values():
        assert accession_id.startswith("GHGAF")


@pytest.mark.asyncio
async def test_publish_study_publishes_aem_event(
    controller, event_store, complete_study_id
):
    """Publishing must emit an AnnotatedExperimentalMetadata event."""
    sid = complete_study_id
    await controller.studies.publish_study(study_id=sid)

    topic = event_store.topics["annotated_metadata"]
    assert len(topic) == 1
    aem = topic[0].payload
    assert aem["study"]["id"] == sid
    assert aem["study"]["title"] == "Full Study"
    assert aem["study"]["publication"] is not None
    assert aem["study"]["publication"]["title"] == "Research Paper"
    assert len(aem["datasets"]) == 1
    assert aem["datasets"][0]["dap"] is not None
    assert aem["datasets"][0]["dap"]["dac"]["id"] == "DAC-1"


@pytest.mark.asyncio
async def test_publish_study_aem_has_accessions(
    controller, event_store, complete_study_id
):
    """The AEM event must contain the generated accession maps."""
    sid = complete_study_id
    await controller.studies.publish_study(study_id=sid)

    aem = event_store.topics["annotated_metadata"][0].payload
    assert "files" in aem["accessions"]
    assert "samples" in aem["accessions"]
    assert "individuals" in aem["accessions"]


@pytest.mark.asyncio
async def test_publish_study_not_found(controller):
    """Publishing a non-existent study must raise StudyNotFoundError."""
    with pytest.raises(StudyRegistryPort.StudyNotFoundError):
        await controller.studies.publish_study(study_id="NONEXIST")


@pytest.mark.asyncio
async def test_publish_study_missing_metadata(controller):
    """Publishing a study without EM must raise ValidationError."""
    study = await controller.studies.create_study(
        data={**E["studies"]["minimal"], "created_by": USER_SUBMITTER},
    )
    # No metadata added
    await controller.publications.create_publication(
        data={**E["publications"]["minimal"], "study_id": study.id},
    )
    with pytest.raises(StudyRegistryPort.ValidationError):
        await controller.studies.publish_study(study_id=study.id)


@pytest.mark.asyncio
async def test_publish_study_republish_generates_new_accessions(
    controller, em_accession_map_dao, complete_study_id
):
    """Republishing must regenerate EM accessions (new accession IDs)."""
    sid = complete_study_id
    await controller.studies.publish_study(study_id=sid)
    first_map = await em_accession_map_dao.get_by_id(sid)
    first_file_accessions = set(first_map.maps["files"].values())

    await controller.studies.publish_study(study_id=sid)
    second_map = await em_accession_map_dao.get_by_id(sid)
    second_file_accessions = set(second_map.maps["files"].values())

    # The second publish should generate different accessions
    assert first_file_accessions != second_file_accessions
