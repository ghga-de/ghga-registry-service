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

"""FastAPI routes for Accessions."""

import logging

from fastapi import APIRouter

from srs.adapters.inbound.fastapi_ import dummies
from srs.adapters.inbound.fastapi_.http_exceptions import (
    HttpAccessionNotFoundError,
    HttpInternalError,
)
from srs.core.models import (
    Accession,
    AltAccession,
    AltAccessionType,
)
from srs.ports.inbound.accession import AccessionPort

log = logging.getLogger(__name__)

router = APIRouter(tags=["Accessions"])

# ──────────────────────── Accessions ────────────────────────────


@router.get(
    "/accessions/{accession_id}",
    summary="Get a primary accession",
    operation_id="getAccession",
    response_model=Accession,
)
async def get_accession(
    accession_id: str,
    registry: dummies.StudyRegistryDummy,
):
    """Look up a primary accession."""
    try:
        return await registry.accessions.get_accession(accession_id=accession_id)
    except AccessionPort.AccessionNotFoundError as err:
        raise HttpAccessionNotFoundError(accession_id=accession_id) from err
    except Exception as err:
        log.exception("Unexpected error in get_accession")
        raise HttpInternalError() from err


@router.get(
    "/accessions/{accession_id}/alt/{alt_type}",
    summary="Get an alternative accession",
    operation_id="getAltAccession",
    response_model=AltAccession,
)
async def get_alt_accession(
    accession_id: str,
    alt_type: AltAccessionType,
    registry: dummies.StudyRegistryDummy,
):
    """Look up an alternative accession (e.g. EGA, FILE_ID)."""
    try:
        return await registry.accessions.get_alt_accession(
            accession_id=accession_id, alt_type=alt_type
        )
    except AccessionPort.AccessionNotFoundError as err:
        raise HttpAccessionNotFoundError(accession_id=accession_id) from err
    except Exception as err:
        log.exception("Unexpected error in get_alt_accession")
        raise HttpInternalError() from err
