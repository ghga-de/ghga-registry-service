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
"""GHGA Registry implementation"""

from rs.ports.inbound.legacy_resources import LegacyResourceManagerPort
from rs.ports.inbound.rdub_manager import RDUBManagerPort
from rs.ports.inbound.registry import RegistryPort
from rs.ports.outbound.dao import StudyDao


class Registry(RegistryPort):
    """Top-level class linking all constituent registry operations"""

    def __init__(
        self,
        *,
        rdub_manager: RDUBManagerPort,
        legacy_resource_manager: LegacyResourceManagerPort,
        study_dao: StudyDao,
    ) -> None:
        self._rdub_manager = rdub_manager
        # LEGACY: see LegacyResourceManager. Remove once this service owns studies and
        # experimental metadata and no longer needs to fetch legacy searchable resources.
        self._legacy_resource_manager = legacy_resource_manager
        # Kept private for internal use by future study operations (e.g. listing
        # studies); intentionally not exposed on the RegistryPort.
        self._study_dao = study_dao

    @property
    def rdub_manager(self) -> RDUBManagerPort:
        """The RDUBManager component."""
        return self._rdub_manager

    @property
    def legacy_resource_manager(self) -> LegacyResourceManagerPort:
        """The LegacyResourceManager component."""
        return self._legacy_resource_manager
