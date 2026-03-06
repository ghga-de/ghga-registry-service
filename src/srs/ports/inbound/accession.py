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

"""Defines the Accession inbound port for accession lookups."""

from abc import ABC, abstractmethod

from srs.core.models import Accession, AltAccession, AltAccessionType
from srs.ports.inbound.errors import AccessionNotFoundError


class AccessionPort(ABC):
    """Inbound port for accession lookup operations."""

    AccessionNotFoundError = AccessionNotFoundError

    @abstractmethod
    async def get_accession(self, *, accession_id: str) -> Accession:
        """Get a primary accession by ID.

        Raises:
        - AccessionNotFoundError if the accession does not exist.
        """
        ...

    @abstractmethod
    async def get_alt_accession(
        self, *, accession_id: str, alt_type: AltAccessionType
    ) -> AltAccession:
        """Get an alternative accession by ID and type.

        Raises:
        - AccessionNotFoundError if the alt accession does not exist.
        """
        ...
