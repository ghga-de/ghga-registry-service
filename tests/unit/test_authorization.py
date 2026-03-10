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

"""Role-based authorization tests for every endpoint.

Each row in ENDPOINTS declares which roles are authorized to call an endpoint.
A single parametrized test checks every (method, path, role) combination
against the live FastAPI app backed by in-memory DAOs, using real JWT tokens
validated by the JWTAuthContextProvider middleware (no dependency overrides).

Roles:
  steward         – data-steward flag is True
  submitter       – the user who created the private study
  other           – an unrelated user not on any study's users list
  unauthenticated – no bearer token at all
"""

from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from ghga_service_commons.api.testing import AsyncTestClient
from ghga_service_commons.utils.jwt_helpers import sign_and_serialize_token

from srs.inject import prepare_rest_app
from tests.conftest import USER_OTHER, USER_STEWARD, USER_SUBMITTER
from tests.fixtures import ConfigFixture
from tests.fixtures.examples import EXAMPLES

E = EXAMPLES


# ── Token helpers ───────────────────────────────────────────────


def _claims_for_role(role: str) -> dict:
    """Return JWT claims for the given role name."""
    now = datetime.now(timezone.utc)
    base = {"iat": now, "exp": now + timedelta(hours=1)}
    claim_data = E["authorization"]["claims"].get(role)
    if claim_data is None:
        raise ValueError(f"Unknown role: {role}")
    user_ids = {"steward": USER_STEWARD, "submitter": USER_SUBMITTER, "other": USER_OTHER}
    return {**base, **claim_data, "id": str(user_ids[role])}


def _headers_for_token(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


ROLES = ("steward", "submitter", "other", "unauthenticated")

# Authenticated roles (all except unauthenticated)
STEWARD = frozenset({"steward"})
ALL = frozenset(ROLES)


# ── Endpoint authorization matrix ───────────────────────────────
#
#  (method, path, authorized_roles)
#
#  authorized_roles – set of role names that must NOT receive 403.
#  Roles outside the set must receive exactly 403.

ENDPOINTS: list[tuple[str, str, frozenset[str]]] = [
    # Studies
    ("POST",   "/studies",                                STEWARD),
    ("GET",    "/studies",                                ALL),
    ("GET",    "/studies/{study_id}",                     frozenset({"steward", "submitter"})),
    ("PATCH",  "/studies/{study_id}",                     STEWARD),
    ("DELETE", "/studies/{study_id}",                     STEWARD),
    # Metadata
    ("PUT",    "/studies/{study_id}/metadata",            STEWARD),
    ("GET",    "/studies/{study_id}/metadata",            STEWARD),
    ("DELETE", "/studies/{study_id}/metadata",            STEWARD),
    # Publications
    ("POST",   "/studies/{study_id}/publications",        STEWARD),
    ("GET",    "/publications",                           ALL),
    ("GET",    "/publications/{publication_id}",          frozenset({"steward", "submitter"})),
    ("DELETE", "/publications/{publication_id}",          STEWARD),
    # DACs
    ("POST",   "/dacs",                                  STEWARD),
    ("GET",    "/dacs",                                  ALL),
    ("GET",    "/dacs/{dac_id}",                         ALL),
    ("PATCH",  "/dacs/{dac_id}",                         STEWARD),
    ("DELETE", "/dacs/{dac_del_id}",                     STEWARD),
    # DAPs
    ("POST",   "/daps",                                  STEWARD),
    ("GET",    "/daps",                                  ALL),
    ("GET",    "/daps/{dap_id}",                         ALL),
    ("PATCH",  "/daps/{dap_id}",                         STEWARD),
    ("DELETE", "/daps/{dap_del_id}",                     STEWARD),
    # Datasets
    ("POST",   "/studies/{study_id}/datasets",           STEWARD),
    ("GET",    "/datasets",                              ALL),
    ("GET",    "/datasets/{dataset_id}",                 frozenset({"steward", "submitter"})),
    ("PATCH",  "/datasets/{dataset_id}",                 STEWARD),
    ("DELETE", "/datasets/{dataset_id}",                 STEWARD),
    # Resource Types
    ("POST",   "/resource-types",                        STEWARD),
    ("GET",    "/resource-types",                        ALL),
    ("GET",    "/resource-types/{resource_type_id}",     ALL),
    ("PATCH",  "/resource-types/{resource_type_id}",     STEWARD),
    ("DELETE", "/resource-types/{resource_type_id}",     STEWARD),
    # Accessions
    ("GET",    "/accessions/{accession_id}",             ALL),
    # Filenames
    ("POST",   "/filenames/{study_id}",                  STEWARD),
    # Publish
    ("POST",   "/studies/{study_id}/publish",            STEWARD),
]


# ── Request bodies for write endpoints ──────────────────────────

REQUEST_BODIES: dict[str, dict] = E["authorization"]["request_bodies"]


# ── Build parametrize list ──────────────────────────────────────

_CASES = [
    pytest.param(
        method,
        path,
        role,
        role in authorized,
        id=f"{method} {path} {role} {'authorized' if role in authorized else 'denied'}",
    )
    for method, path, authorized in ENDPOINTS
    for role in ROLES
]


# ── Fixtures ────────────────────────────────────────────────────


@pytest_asyncio.fixture()
async def seeded_app(config: ConfigFixture, controller):
    """Build a FastAPI app with real JWT auth and seeded data.

    Uses prepare_rest_app with controller_override so the real
    JWTAuthContextProvider validates tokens while the core is in-memory.
    """
    _seed = E["authorization"]["seed"]
    # Seed data through the controller
    study = await controller.studies.create_study(
        data={**_seed["study"], "created_by": USER_SUBMITTER},
    )
    await controller.metadata.upsert_metadata(
        study_id=study.id, metadata=_seed["metadata"],
    )
    pub = await controller.publications.create_publication(
        data={**_seed["publication"], "study_id": study.id},
    )
    await controller.data_access.create_dac(data=E["dacs"]["default"])
    await controller.data_access.create_dap(data=E["daps"]["default"])
    await controller.data_access.create_dac(data=E["dacs"]["deletable"])
    await controller.data_access.create_dap(data=E["daps"]["deletable"])
    ds = await controller.datasets.create_dataset(
        data={**_seed["dataset"], "study_id": study.id, "dap_id": "DAP-1"},
    )
    rt = await controller.resource_types.create_resource_type(data=_seed["resource_type"])

    ids = {
        "study_id": study.id,
        "publication_id": pub.id,
        "dataset_id": ds.id,
        "dac_id": "DAC-1",
        "dac_del_id": "DAC-DEL",
        "dap_id": "DAP-1",
        "dap_del_id": "DAP-DEL",
        "resource_type_id": str(rt.id),
        "accession_id": study.id,
    }

    async with prepare_rest_app(
        config=config.config, controller_override=controller
    ) as app:
        async with AsyncTestClient(app=app) as client:
            yield client, ids


# ── The test ────────────────────────────────────────────────────


@pytest.mark.asyncio(loop_scope="function")
@pytest.mark.parametrize("method, path, role, authorized", _CASES)
async def test_authorization(
    config: ConfigFixture, seeded_app, method, path, role, authorized
):
    """Every (method, path, role) must be authorized or denied as declared."""
    client, ids = seeded_app
    url = path.format(**ids)
    body = REQUEST_BODIES.get(f"{method} {path}")

    # Build headers: real JWT for authenticated roles, none otherwise
    headers: dict[str, str] = {}
    if role != "unauthenticated":
        claims = _claims_for_role(role)
        token = sign_and_serialize_token(claims, config.jwk)
        headers = _headers_for_token(token)

    response = await client.request(method, url, json=body, headers=headers)

    if authorized:
        assert response.status_code not in (401, 403), (
            f"{method} {url} as {role}: expected authorized, got {response.status_code}"
        )
    else:
        assert response.status_code in (401, 403), (
            f"{method} {url} as {role}: expected 401/403, got {response.status_code}"
        )
