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

"""Accession number generation following the legacy GHGA accession format.

Format: GHGA{letter}{14-digit random number}
Example: GHGAS12345678901234  (Study)
"""

import random

from srs.constants import ACCESSION_BASE, ACCESSION_DIGITS, ACCESSION_PREFIXES
from srs.core.models import AccessionType


def generate_accession(accession_type: AccessionType) -> str:
    """Generate a new legacy GHGA accession number for the given type.

    Format: GHGA + prefix letter + 14 random digits.
    """
    prefix = ACCESSION_PREFIXES.get(accession_type.value)
    if prefix is None:
        msg = f"No accession prefix defined for type {accession_type}"
        raise ValueError(msg)
    digits = "".join(str(random.randint(0, 9)) for _ in range(ACCESSION_DIGITS))  # noqa: S311
    return f"{ACCESSION_BASE}{prefix}{digits}"


# Mapping from EM resource names (as used in schemapack) to AccessionType
EM_RESOURCE_TO_ACCESSION_TYPE: dict[str, AccessionType] = {
    "analyses": AccessionType.ANALYSIS,
    "analysis_methods": AccessionType.ANALYSIS_METHOD,
    "experiments": AccessionType.EXPERIMENT,
    "experiment_methods": AccessionType.EXPERIMENT_METHOD,
    "files": AccessionType.FILE,
    "individuals": AccessionType.INDIVIDUAL,
    "samples": AccessionType.SAMPLE,
}
