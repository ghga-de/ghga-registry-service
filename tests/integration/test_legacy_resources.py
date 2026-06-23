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

"""Testing the consumer for legacy searchable resource events.

LEGACY: covers the consumer that fetches searchable resources from metldata.
The consumer validates and routes the events and extracts the embedded study
into the study DAO. Remove together with the consumer once this service owns studies
and experimental metadata itself.
"""

from uuid import UUID

import pytest

from rs.constants import STUDY_COLLECTION
from rs.core.legacy_resources import _LEGACY_CREATED_BY
from rs.core.models import StudyStatus
from tests.fixtures.joint import JointFixture

pytestmark = pytest.mark.asyncio


def _study_collection(joint_fixture: JointFixture):
    """Return the MongoDB collection backing the study DAO."""
    db = joint_fixture.mongodb.client[joint_fixture.config.db_name]
    return db[STUDY_COLLECTION]


def _study_count(joint_fixture: JointFixture) -> int:
    """Count the studies currently stored in the study DAO collection."""
    return _study_collection(joint_fixture).count_documents({})


def _embedded_dataset_payload(study_accession: str = "GHGAS78925948359890") -> dict:
    """Build a searchable-resource payload embedding a study (as metldata emits)."""
    return {
        "accession": "GHGAD64952591994356",
        "class_name": "EmbeddedDataset",
        "content": {
            "accession": "GHGAD64952591994356",
            "title": "DS dataset",
            "study": {
                "accession": study_accession,
                "alias": "STUDY_bRFg_B",
                "title": "The bRFg_B Study",
                "description": "The study with alias STUDY_bRFg_B",
                "types": ["WHOLE_GENOME_SEQUENCING"],
                "ega_accession": "EGASTUDY12346",
                "affiliations": ["Some Institute"],
                "datasets": ["GHGAD64952591994356"],
                "publications": [],
            },
        },
    }


async def test_searchable_resource_upsertion_stores_study(joint_fixture: JointFixture):
    """An upsertion event extracts the embedded study and stores it in the study DAO."""
    config = joint_fixture.config

    await joint_fixture.kafka.publish_event(
        payload=_embedded_dataset_payload(),
        type_=config.resource_upsertion_type,
        topic=config.resource_change_topic,
        key="dataset_embedded_GHGAD64952591994356",
    )
    await joint_fixture.event_subscriber.run(forever=False)

    assert _study_count(joint_fixture) == 1
    stored = _study_collection(joint_fixture).find_one({"_id": "GHGAS78925948359890"})
    assert stored is not None
    assert stored["title"] == "The bRFg_B Study"
    assert stored["types"] == ["WHOLE_GENOME_SEQUENCING"]
    assert stored["affiliations"] == ["Some Institute"]
    assert stored["status"] == StudyStatus.ARCHIVED
    assert UUID(str(stored["created_by"])) == _LEGACY_CREATED_BY
    assert stored["num_datasets"] == 1
    assert stored["num_publications"] == 0


async def test_existing_study_is_not_updated(joint_fixture: JointFixture):
    """Re-consuming a known study (e.g. via another embedded dataset) leaves it intact.

    Studies are only created via this mechanism, never updated, so the originally
    stored record - including its ``created`` timestamp - must be preserved even when a
    later event carries changed study content.
    """
    config = joint_fixture.config

    await joint_fixture.kafka.publish_event(
        payload=_embedded_dataset_payload(),
        type_=config.resource_upsertion_type,
        topic=config.resource_change_topic,
        key="dataset_embedded_GHGAD64952591994356",
    )
    await joint_fixture.event_subscriber.run(forever=False)
    stored_first = _study_collection(joint_fixture).find_one(
        {"_id": "GHGAS78925948359890"}
    )
    assert stored_first is not None

    # A second event carrying the same study id but a different title must not update it.
    changed = _embedded_dataset_payload()
    changed["content"]["study"]["title"] = "A different title"
    await joint_fixture.kafka.publish_event(
        payload=changed,
        type_=config.resource_upsertion_type,
        topic=config.resource_change_topic,
        key="dataset_embedded_GHGAD64952591994356",
    )
    await joint_fixture.event_subscriber.run(forever=False)

    assert _study_count(joint_fixture) == 1
    stored_second = _study_collection(joint_fixture).find_one(
        {"_id": "GHGAS78925948359890"}
    )
    assert stored_second == stored_first
    assert stored_second["title"] == "The bRFg_B Study"


async def test_searchable_resource_without_study_is_ignored(
    joint_fixture: JointFixture,
):
    """A resource whose content has no embedded study persists nothing."""
    config = joint_fixture.config

    await joint_fixture.kafka.publish_event(
        payload={
            "accession": "GHGA:RESOURCE1",
            "class_name": "ExperimentMethod",
            "content": {"title": "Some resource", "nested": {"value": 1}},
        },
        type_=config.resource_upsertion_type,
        topic=config.resource_change_topic,
        key="experiment_method_GHGA:RESOURCE1",
    )
    await joint_fixture.event_subscriber.run(forever=False)

    assert _study_count(joint_fixture) == 0


async def test_searchable_resource_deletion_is_consumed(joint_fixture: JointFixture):
    """A deletion event (carrying only SearchableResourceInfo) is consumed without error."""
    config = joint_fixture.config

    await joint_fixture.kafka.publish_event(
        payload={"accession": "GHGA:STUDY1", "class_name": "Study"},
        type_=config.resource_deletion_type,
        topic=config.resource_change_topic,
        key="dataset_embedded_GHGA:STUDY1",
    )
    await joint_fixture.event_subscriber.run(forever=False)

    assert _study_count(joint_fixture) == 0
