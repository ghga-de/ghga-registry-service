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

"""Constants used across the Study Registry Service."""

from opentelemetry import trace

SERVICE_NAME = "srs"

# MongoDB collection names
STUDIES_COLLECTION = "studies"
EXPERIMENTAL_METADATA_COLLECTION = "experimentalMetadata"
PUBLICATIONS_COLLECTION = "publications"
DACS_COLLECTION = "dataAccessCommittees"
DAPS_COLLECTION = "dataAccessPolicies"
DATASETS_COLLECTION = "datasets"
RESOURCE_TYPES_COLLECTION = "resourceTypes"
ACCESSIONS_COLLECTION = "accessions"
ALT_ACCESSIONS_COLLECTION = "altAccessions"
EM_ACCESSION_MAPS_COLLECTION = "emAccessionMaps"

# Accession prefix mapping: AccessionType -> prefix letter
ACCESSION_PREFIXES: dict[str, str] = {
    "STUDY": "S",
    "DATASET": "D",
    "PUBLICATION": "U",
    "EXPERIMENT": "X",
    "EXPERIMENT_METHOD": "Q",
    "SAMPLE": "N",
    "INDIVIDUAL": "I",
    "FILE": "F",
    "ANALYSIS": "A",
    "ANALYSIS_METHOD": "M",
}

ACCESSION_BASE = "GHGA"
ACCESSION_DIGITS = 14

TRACER = trace.get_tracer(SERVICE_NAME)
