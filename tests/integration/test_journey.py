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

"""End-to-end journey test: New Study lifecycle.

Uses the JointFixture backed by real MongoDB and Kafka containers.

Covers the full workflow:
  1. Create study
  2. Upsert experimental metadata
  3. Create publication
  4. Create DAC
  5. Create DAP
  6. Create dataset
  7. Publish study (generates accessions + AEM event)
  8. Post filenames (stores file IDs + publishes mapping event)
  9. Persist study (status PENDING → PERSISTED)
  10. Republish study
"""

import pytest

from srs.core.models import StudyStatus
from srs.ports.inbound.study_registry import StudyRegistryPort
from tests.conftest import USER_OTHER, USER_STEWARD, USER_SUBMITTER
from tests.fixtures.examples import EXAMPLES
from tests.fixtures.joint import JointFixture

pytestmark = pytest.mark.asyncio(loop_scope="function")

E = EXAMPLES


async def test_new_study_journey(joint_fixture: JointFixture):
    """Full lifecycle: create → publish → filenames → persist → republish."""
    controller = joint_fixture.controller
    kafka = joint_fixture.kafka

    # 1 ── Create study ───────────────────────────────────────────
    study = await controller.create_study(
        **E["studies"]["journey"], created_by=USER_SUBMITTER,
    )
    assert study.status == StudyStatus.PENDING
    assert study.id.startswith("GHGAS")
    study_id = study.id

    # 2 ── Upsert experimental metadata ──────────────────────────
    await controller.upsert_metadata(
        study_id=study_id, metadata=E["metadata"]["journey"],
    )
    em = await controller.get_metadata(study_id=study_id)
    assert em.metadata == E["metadata"]["journey"]

    # 3 ── Create publication ────────────────────────────────────
    pub = await controller.create_publication(
        **E["publications"]["journey"], study_id=study_id,
    )
    assert pub.id.startswith("GHGAU")
    assert pub.study_id == study_id

    # 4 ── Create DAC ────────────────────────────────────────────
    await controller.create_dac(**E["dacs"]["journey"])
    dac = await controller.get_dac(dac_id="DAC-DIABETES")
    assert dac.active is True

    # 5 ── Create DAP ────────────────────────────────────────────
    await controller.create_dap(**E["daps"]["journey"])
    dap = await controller.get_dap(dap_id="DAP-DIABETES")
    assert dap.dac_id == "DAC-DIABETES"

    # 6 ── Create dataset ────────────────────────────────────────
    ds = await controller.create_dataset(
        **E["datasets"]["journey"], study_id=study_id, dap_id="DAP-DIABETES",
    )
    assert ds.id.startswith("GHGAD")
    assert ds.study_id == study_id

    # 7 ── Publish study ─────────────────────────────────────────
    async with kafka.record_events(
        in_topic=joint_fixture.config.annotated_metadata_topic,
    ) as recorder:
        await controller.publish_study(study_id=study_id)

    assert len(recorder.recorded_events) == 1
    aem_payload = recorder.recorded_events[0].payload
    assert aem_payload["study"]["id"] == study_id
    assert aem_payload["study"]["publication"]["doi"] == E["publications"]["journey"]["doi"]
    assert len(aem_payload["datasets"]) == 1
    assert aem_payload["datasets"][0]["dap"]["dac"]["id"] == "DAC-DIABETES"
    assert len(aem_payload["accessions"]["files"]) == 2
    assert len(aem_payload["accessions"]["samples"]) == 2

    # 8 ── Post filenames ────────────────────────────────────────
    filenames = await controller.get_filenames(study_id=study_id)
    assert len(filenames) == 2

    file_acc_ids = list(filenames.keys())
    file_id_map = {
        file_acc_ids[0]: "s3://bucket/file_a.bam",
        file_acc_ids[1]: "s3://bucket/file_b.bam",
    }
    async with kafka.record_events(
        in_topic=joint_fixture.config.file_id_mapping_topic,
    ) as mapping_recorder:
        await controller.post_filenames(
            study_id=study_id, file_id_map=file_id_map
        )

    assert len(mapping_recorder.recorded_events) == 1
    assert mapping_recorder.recorded_events[0].payload["mapping"] == file_id_map

    # 9 ── Persist study (PENDING → PERSISTED) ───────────────────
    await controller.update_study(
        study_id=study_id,
        status=StudyStatus.PERSISTED,
        approved_by=USER_STEWARD,
    )
    persisted = await controller.get_study(
        study_id=study_id,
        user_id=USER_STEWARD,
        is_data_steward=True,
    )
    assert persisted.status == StudyStatus.PERSISTED
    assert persisted.approved_by == USER_STEWARD

    # 10 ── Republish study ──────────────────────────────────────
    async with kafka.record_events(
        in_topic=joint_fixture.config.annotated_metadata_topic,
    ) as recorder2:
        await controller.publish_study(study_id=study_id)

    assert len(recorder2.recorded_events) == 1
    aem2_payload = recorder2.recorded_events[0].payload
    assert aem2_payload["study"]["id"] == study_id
    # New publish generates new accessions
    assert aem2_payload["accessions"]["files"] != aem_payload["accessions"]["files"]


async def test_journey_delete_pending_study_cleans_everything(
    joint_fixture: JointFixture,
):
    """Deleting a PENDING study must cascade-remove all related entities."""
    controller = joint_fixture.controller

    study = await controller.create_study(
        **E["studies"]["minimal"], created_by=USER_SUBMITTER,
    )
    sid = study.id

    await controller.upsert_metadata(
        study_id=sid, metadata=E["metadata"]["with_named_file"],
    )
    pub = await controller.create_publication(
        **E["publications"]["minimal"], study_id=sid,
    )
    await controller.create_dac(**E["dacs"]["tmp"])
    await controller.create_dap(**E["daps"]["tmp"])
    ds = await controller.create_dataset(
        **E["datasets"]["with_files"], study_id=sid, dap_id="DAP-TMP",
    )

    # Delete the study
    await controller.delete_study(study_id=sid)

    # Verify study and related entities are gone
    with pytest.raises(StudyRegistryPort.StudyNotFoundError):
        await controller.get_study(
            study_id=sid, user_id=USER_STEWARD, is_data_steward=True
        )
    with pytest.raises(StudyRegistryPort.MetadataNotFoundError):
        await controller.get_metadata(study_id=sid)
    with pytest.raises(StudyRegistryPort.PublicationNotFoundError):
        await controller.get_publication(publication_id=pub.id)
    with pytest.raises(StudyRegistryPort.DatasetNotFoundError):
        await controller.get_dataset(
            dataset_id=ds.id, user_id=USER_STEWARD, is_data_steward=True
        )

    # DAC and DAP should still exist (they are independent)
    dac = await controller.get_dac(dac_id="DAC-TMP")
    assert dac.name == "Board"


async def test_unhappy_journey(joint_fixture: JointFixture):
    """Unhappy path: every step that can go wrong does go wrong first.

    Walks through the study lifecycle hitting every error condition:
      - non-existent resources (404)
      - status conflicts (409)
      - reference conflicts (409)
      - access control (403)
      - validation failures (422)
      - duplicate detection (409)
    before eventually recovering and completing the happy path.
    """
    controller = joint_fixture.controller
    kafka = joint_fixture.kafka

    # ── 1. Operate on non-existent study ─────────────────────────
    with pytest.raises(StudyRegistryPort.StudyNotFoundError):
        await controller.get_study(study_id="GHGAS_FAKE", user_id=USER_SUBMITTER)

    with pytest.raises(StudyRegistryPort.StudyNotFoundError):
        await controller.upsert_metadata(study_id="GHGAS_FAKE", metadata={})

    with pytest.raises(StudyRegistryPort.StudyNotFoundError):
        await controller.delete_study(study_id="GHGAS_FAKE")

    with pytest.raises(StudyRegistryPort.StudyNotFoundError):
        await controller.publish_study(study_id="GHGAS_FAKE")

    # ── 2. Create a study successfully ───────────────────────────
    study = await controller.create_study(
        **E["studies"]["unhappy"], created_by=USER_SUBMITTER,
    )
    sid = study.id

    # ── 3. Access control: unauthorized user blocked ─────────────
    with pytest.raises(StudyRegistryPort.AccessDeniedError):
        await controller.get_study(study_id=sid, user_id=USER_OTHER)

    # ── 4. Publish without metadata or publication → validation ──
    with pytest.raises(StudyRegistryPort.ValidationError):
        await controller.publish_study(study_id=sid)

    # ── 5. Persist without completeness → validation ─────────────
    with pytest.raises(StudyRegistryPort.StatusConflictError):
        await controller.update_study(
            study_id=sid, status=StudyStatus.FROZEN
        )
    with pytest.raises(StudyRegistryPort.ValidationError):
        await controller.update_study(
            study_id=sid, status=StudyStatus.PERSISTED
        )

    # ── 6. Add metadata, still no publication → publish fails ────
    await controller.upsert_metadata(
        study_id=sid, metadata=E["metadata"]["with_named_file"],
    )
    with pytest.raises(StudyRegistryPort.ValidationError):
        await controller.publish_study(study_id=sid)

    # ── 7. Publication on non-existent study ─────────────────────
    with pytest.raises(StudyRegistryPort.StudyNotFoundError):
        await controller.create_publication(
            **E["publications"]["minimal"], study_id="GHGAS_FAKE",
        )

    # ── 8. Add publication → now publish succeeds ────────────────
    pub = await controller.create_publication(
        **E["publications"]["paper"], study_id=sid,
    )

    # ── 9. DAP with non-existent DAC ────────────────────────────
    with pytest.raises(StudyRegistryPort.DacNotFoundError):
        await controller.create_dap(
            **{**E["daps"]["default"], "id": "DAP-BAD", "dac_id": "DAC-NONEXIST"},
        )

    # ── 10. Create DAC, then duplicate → error ──────────────────
    await controller.create_dac(**E["dacs"]["default"])
    with pytest.raises(StudyRegistryPort.DuplicateError):
        await controller.create_dac(
            **{**E["dacs"]["default"], "name": "Dup", "email": "x@example.org", "institute": "Y"},
        )

    # ── 11. Create DAP, then duplicate → error ──────────────────
    await controller.create_dap(**E["daps"]["default"])
    with pytest.raises(StudyRegistryPort.DuplicateError):
        await controller.create_dap(
            **{**E["daps"]["default"], "name": "Dup"},
        )

    # ── 12. Dataset with non-existent DAP ────────────────────────
    with pytest.raises(StudyRegistryPort.DapNotFoundError):
        await controller.create_dataset(
            **E["datasets"]["minimal"], study_id=sid, dap_id="DAP-NONEXIST",
        )

    # ── 13. Dataset with duplicate file aliases ──────────────────
    with pytest.raises(StudyRegistryPort.ValidationError):
        await controller.create_dataset(
            **{**E["datasets"]["minimal"], "files": ["f1", "f1"]},
            study_id=sid, dap_id="DAP-1",
        )

    # ── 14. Create dataset successfully ──────────────────────────
    ds = await controller.create_dataset(
        **{**E["datasets"]["with_files"], "types": ["WGS"]},
        study_id=sid, dap_id="DAP-1",
    )

    # ── 15. Delete DAC blocked by DAP reference ──────────────────
    with pytest.raises(StudyRegistryPort.ReferenceConflictError):
        await controller.delete_dac(dac_id="DAC-1")

    # ── 16. Delete DAP blocked by dataset reference ──────────────
    with pytest.raises(StudyRegistryPort.ReferenceConflictError):
        await controller.delete_dap(dap_id="DAP-1")

    # ── 17. Publish study (now complete) ─────────────────────────
    async with kafka.record_events(
        in_topic=joint_fixture.config.annotated_metadata_topic,
    ) as recorder:
        await controller.publish_study(study_id=sid)

    assert len(recorder.recorded_events) == 1

    # ── 18. Post filenames with invalid accession ────────────────
    with pytest.raises(StudyRegistryPort.ValidationError):
        await controller.post_filenames(
            study_id=sid,
            file_id_map={"GHGAF_INVALID_00000": "some-id"},
        )

    # ── 19. Post filenames with valid accession ──────────────────
    filenames = await controller.get_filenames(study_id=sid)
    file_acc_ids = list(filenames.keys())
    file_id_map = {acc: f"s3://bucket/{acc}" for acc in file_acc_ids}
    async with kafka.record_events(
        in_topic=joint_fixture.config.file_id_mapping_topic,
    ) as mapping_recorder:
        await controller.post_filenames(study_id=sid, file_id_map=file_id_map)

    assert len(mapping_recorder.recorded_events) == 1

    # ── 20. Persist study ────────────────────────────────────────
    await controller.update_study(
        study_id=sid,
        status=StudyStatus.PERSISTED,
        approved_by=USER_STEWARD,
    )
    persisted = await controller.get_study(
        study_id=sid, user_id=USER_STEWARD, is_data_steward=True
    )
    assert persisted.status == StudyStatus.PERSISTED

    # ── 21. Mutations blocked on PERSISTED study ─────────────────
    with pytest.raises(StudyRegistryPort.StatusConflictError):
        await controller.upsert_metadata(study_id=sid, metadata={"new": True})

    with pytest.raises(StudyRegistryPort.StatusConflictError):
        await controller.delete_metadata(study_id=sid)

    with pytest.raises(StudyRegistryPort.StatusConflictError):
        await controller.create_publication(
            **E["publications"]["minimal"], study_id=sid,
        )

    with pytest.raises(StudyRegistryPort.StatusConflictError):
        await controller.delete_publication(publication_id=pub.id)

    with pytest.raises(StudyRegistryPort.StatusConflictError):
        await controller.create_dataset(
            **E["datasets"]["minimal"], study_id=sid, dap_id="DAP-1",
        )

    with pytest.raises(StudyRegistryPort.StatusConflictError):
        await controller.delete_dataset(dataset_id=ds.id)

    with pytest.raises(StudyRegistryPort.StatusConflictError):
        await controller.delete_study(study_id=sid)

    # ── 22. Persisting again is also invalid ─────────────────────
    with pytest.raises(StudyRegistryPort.StatusConflictError):
        await controller.update_study(
            study_id=sid, status=StudyStatus.PERSISTED
        )

    # ── 23. Republish still works on PERSISTED study ─────────────
    async with kafka.record_events(
        in_topic=joint_fixture.config.annotated_metadata_topic,
    ) as recorder2:
        await controller.publish_study(study_id=sid)

    assert len(recorder2.recorded_events) == 1
