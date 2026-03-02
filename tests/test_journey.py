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
from tests.conftest import USER_STEWARD, USER_SUBMITTER


@pytest.mark.asyncio
async def test_new_study_journey(controller, event_publisher, study_dao):
    """Full lifecycle: create → publish → filenames → persist → republish."""

    # 1 ── Create study ───────────────────────────────────────────
    study = await controller.create_study(
        title="Genome-wide Association Study on Diabetes",
        description="A GWAS on type 2 diabetes in the European population.",
        types=["WGS", "GWAS"],
        affiliations=["GHGA", "Heidelberg University"],
        created_by=USER_SUBMITTER,
    )
    assert study.status == StudyStatus.PENDING
    assert study.id.startswith("GHGAS")
    study_id = study.id

    # 2 ── Upsert experimental metadata ──────────────────────────
    em_metadata = {
        "files": {
            "file_a": {"name": "reads_sample1.bam", "format": "BAM"},
            "file_b": {"name": "reads_sample2.bam", "format": "BAM"},
        },
        "samples": {
            "sample_1": {"name": "Patient 001"},
            "sample_2": {"name": "Patient 002"},
        },
        "individuals": {
            "ind_1": {"name": "Donor 001", "sex": "male"},
        },
        "experiments": {
            "exp_1": {"name": "WGS run 1"},
        },
    }
    await controller.upsert_metadata(
        study_id=study_id, metadata=em_metadata
    )
    em = await controller.get_metadata(study_id=study_id)
    assert em.metadata == em_metadata

    # 3 ── Create publication ────────────────────────────────────
    pub = await controller.create_publication(
        title="Type 2 Diabetes GWAS in Europe",
        abstract="We identified 42 novel loci associated with T2D.",
        authors=["Alice Researcher", "Bob Analyst"],
        year=2025,
        journal="Nature Genetics",
        doi="10.1038/s41588-025-00001-x",
        study_id=study_id,
    )
    assert pub.id.startswith("GHGAU")
    assert pub.study_id == study_id

    # 4 ── Create DAC ────────────────────────────────────────────
    await controller.create_dac(
        id="DAC-DIABETES",
        name="Diabetes Ethics Committee",
        email="dac-diabetes@example.org",
        institute="Heidelberg University Hospital",
    )
    dac = await controller.get_dac(dac_id="DAC-DIABETES")
    assert dac.active is True

    # 5 ── Create DAP ────────────────────────────────────────────
    await controller.create_dap(
        id="DAP-DIABETES",
        name="Diabetes Data Access Policy",
        description="Access for registered researchers.",
        text="Applicants must provide IRB approval.",
        url="https://policies.example.org/diabetes",
        duo_permission_id="DUO:0000042",  # GRU
        duo_modifier_ids=["DUO:0000021"],  # IRB required
        dac_id="DAC-DIABETES",
    )
    dap = await controller.get_dap(dap_id="DAP-DIABETES")
    assert dap.dac_id == "DAC-DIABETES"

    # 6 ── Create dataset ────────────────────────────────────────
    ds = await controller.create_dataset(
        title="Diabetes WGS Dataset",
        description="BAM files from WGS runs",
        types=["WGS"],
        study_id=study_id,
        dap_id="DAP-DIABETES",
        files=["file_a", "file_b"],
    )
    assert ds.id.startswith("GHGAD")
    assert ds.study_id == study_id

    # 7 ── Publish study ─────────────────────────────────────────
    await controller.publish_study(study_id=study_id)

    assert len(event_publisher.annotated_metadata_events) == 1
    aem = event_publisher.annotated_metadata_events[0]

    # Check AEM structure
    assert aem.study.id == study_id
    assert aem.study.publication is not None
    assert aem.study.publication.doi == "10.1038/s41588-025-00001-x"
    assert len(aem.datasets) == 1
    assert aem.datasets[0].dap.dac.id == "DAC-DIABETES"

    # Check accession maps
    assert "files" in aem.accessions
    assert "samples" in aem.accessions
    assert "individuals" in aem.accessions
    assert "experiments" in aem.accessions
    assert len(aem.accessions["files"]) == 2
    assert len(aem.accessions["samples"]) == 2

    # 8 ── Post filenames ────────────────────────────────────────
    filenames = await controller.get_filenames(study_id=study_id)
    assert len(filenames) == 2

    file_acc_ids = list(filenames.keys())
    file_id_map = {
        file_acc_ids[0]: "s3://bucket/file_a.bam",
        file_acc_ids[1]: "s3://bucket/file_b.bam",
    }
    await controller.post_filenames(
        study_id=study_id, file_id_map=file_id_map
    )

    assert len(event_publisher.file_id_mapping_events) == 1
    assert event_publisher.file_id_mapping_events[0] == file_id_map

    # 9 ── Persist study (PENDING → PERSISTED) ───────────────────
    await controller.update_study(
        study_id=study_id,
        status=StudyStatus.PERSISTED,
        approved_by=USER_STEWARD,
    )
    persisted = await study_dao.get_by_id(study_id)
    assert persisted.status == StudyStatus.PERSISTED
    assert persisted.approved_by == USER_STEWARD

    # 10 ── Republish study ──────────────────────────────────────
    await controller.publish_study(study_id=study_id)
    assert len(event_publisher.annotated_metadata_events) == 2
    aem2 = event_publisher.annotated_metadata_events[1]
    assert aem2.study.id == study_id
    # New publish generates new accessions
    assert aem2.accessions["files"] != aem.accessions["files"]


@pytest.mark.asyncio
async def test_journey_delete_pending_study_cleans_everything(
    controller,
    study_dao,
    metadata_dao,
    publication_dao,
    dataset_dao,
    accession_dao,
):
    """Deleting a PENDING study must cascade-remove all related entities."""
    study = await controller.create_study(
        title="Temp Study",
        description="",
        types=[],
        affiliations=[],
        created_by=USER_SUBMITTER,
    )
    sid = study.id

    await controller.upsert_metadata(
        study_id=sid,
        metadata={"files": {"f1": {"name": "test.txt"}}},
    )
    pub = await controller.create_publication(
        title="Pub",
        abstract=None,
        authors=["A"],
        year=2025,
        journal=None,
        doi=None,
        study_id=sid,
    )
    await controller.create_dac(
        id="DAC-TMP",
        name="Board",
        email="b@example.org",
        institute="I",
    )
    await controller.create_dap(
        id="DAP-TMP",
        name="P",
        description="d",
        text="t",
        url=None,
        duo_permission_id="DUO:0000042",
        duo_modifier_ids=[],
        dac_id="DAC-TMP",
    )
    ds = await controller.create_dataset(
        title="DS",
        description="d",
        types=[],
        study_id=sid,
        dap_id="DAP-TMP",
        files=["f1"],
    )

    # Delete the study
    await controller.delete_study(study_id=sid)

    # Verify everything is gone
    assert sid not in study_dao.data
    assert sid not in metadata_dao.data
    assert pub.id not in publication_dao.data
    assert ds.id not in dataset_dao.data
    assert sid not in accession_dao.data
    assert pub.id not in accession_dao.data
    assert ds.id not in accession_dao.data

    # DAC and DAP should still exist (they are independent)
    dac = await controller.get_dac(dac_id="DAC-TMP")
    assert dac.name == "Board"
