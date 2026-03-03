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
against the live FastAPI app backed by in-memory DAOs.

Roles:
  steward         – data-steward flag is True
  submitter       – the user who created the private study
  other           – an unrelated user not on any study's users list
  unauthenticated – no bearer token at all
"""

from datetime import datetime, timezone
from uuid import UUID

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI, HTTPException
from ghga_service_commons.auth.ghga import AuthContext

from ghga_service_commons.httpyexpect.server.handlers.fastapi_ import (
    configure_exception_handler,
)

from srs.adapters.inbound.fastapi_ import dummies
from srs.adapters.inbound.fastapi_.http_authorization import (
    _optional_auth_context,
    _require_auth_context,
)
from srs.adapters.inbound.fastapi_.routes import router
from tests.conftest import USER_OTHER, USER_STEWARD, USER_SUBMITTER


# ── Auth contexts per role ──────────────────────────────────────

_NOW = datetime.now(timezone.utc)

ROLE_CONTEXTS: dict[str, AuthContext | None] = {
    "steward": AuthContext(
        name="Steward",
        email="steward@test.org",
        id=str(USER_STEWARD),
        roles=["data_steward"],
        iat=_NOW,
        exp=_NOW,
    ),
    "submitter": AuthContext(
        name="Submitter",
        email="submitter@test.org",
        id=str(USER_SUBMITTER),
        roles=[],
        iat=_NOW,
        exp=_NOW,
    ),
    "other": AuthContext(
        name="Other",
        email="other@test.org",
        id=str(USER_OTHER),
        roles=[],
        iat=_NOW,
        exp=_NOW,
    ),
    "unauthenticated": None,
}

# Authenticated roles (all except unauthenticated)
AUTHENTICATED = frozenset({"steward", "submitter", "other"})
STEWARD = frozenset({"steward"})
ALL = frozenset(ROLE_CONTEXTS)


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

REQUEST_BODIES: dict[str, dict] = {
    "POST /studies": {
        "title": "New", "description": "d", "types": [], "affiliations": [],
    },
    "PATCH /studies/{study_id}": {},
    "PUT /studies/{study_id}/metadata": {"metadata": {"key": "val"}},
    "POST /studies/{study_id}/publications": {
        "title": "P", "abstract": None, "authors": ["A"],
        "year": 2025, "journal": None, "doi": None,
    },
    "POST /dacs": {
        "id": "DAC-NEW", "name": "N", "email": "n@x.org", "institute": "I",
    },
    "PATCH /dacs/{dac_id}": {},
    "POST /daps": {
        "id": "DAP-NEW", "name": "N", "description": "d", "text": "t",
        "url": None, "duo_permission_id": "DUO:0000042",
        "duo_modifier_ids": [], "dac_id": "DAC-1",
    },
    "PATCH /daps/{dap_id}": {},
    "POST /studies/{study_id}/datasets": {
        "title": "D", "description": "d", "types": [],
        "dap_id": "DAP-1", "files": [],
    },
    "PATCH /datasets/{dataset_id}": {"dap_id": "DAP-1"},
    "POST /resource-types": {
        "code": "RT-NEW", "resource": "STUDY", "name": "N", "description": "d",
    },
    "PATCH /resource-types/{resource_type_id}": {},
    "POST /filenames/{study_id}": {"file_id_map": {}},
}


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
    for role in ROLE_CONTEXTS
]


# ── Fixtures ────────────────────────────────────────────────────


@pytest_asyncio.fixture()
async def seeded_app(controller):
    """Build a FastAPI app with seeded data and return (app, path_ids)."""
    # Seed a private study owned by USER_SUBMITTER
    study = await controller.create_study(
        title="S", description="d", types=[], affiliations=[],
        created_by=USER_SUBMITTER,
    )
    # Metadata
    await controller.upsert_metadata(
        study_id=study.id, metadata={"key": "val"},
    )
    # Publication
    pub = await controller.create_publication(
        title="P", abstract=None, authors=["A"], year=2025,
        journal=None, doi=None, study_id=study.id,
    )
    # DAC + DAP (used by dataset)
    await controller.create_dac(
        id="DAC-1", name="C", email="c@x.org", institute="I",
    )
    await controller.create_dap(
        id="DAP-1", name="P", description="d", text="t", url=None,
        duo_permission_id="DUO:0000042", duo_modifier_ids=[], dac_id="DAC-1",
    )
    # Unreferenced DAC + DAP for DELETE tests
    await controller.create_dac(
        id="DAC-DEL", name="D", email="d@x.org", institute="I",
    )
    await controller.create_dap(
        id="DAP-DEL", name="D", description="d", text="t", url=None,
        duo_permission_id="DUO:0000042", duo_modifier_ids=[], dac_id="DAC-DEL",
    )
    # Dataset
    ds = await controller.create_dataset(
        title="DS", description="d", types=[], study_id=study.id,
        dap_id="DAP-1", files=[],
    )
    # Resource Type
    rt = await controller.create_resource_type(
        code="RT1", resource="STUDY", name="Type", description="d",
    )

    # Wire the FastAPI app
    app = FastAPI()
    app.include_router(router)
    configure_exception_handler(app)
    app.dependency_overrides[dummies.study_registry_port] = lambda: controller

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
    return app, ids


# ── The test ────────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize("method, path, role, authorized", _CASES)
async def test_authorization(seeded_app, method, path, role, authorized):
    """Every (method, path, role) must be authorized or denied as declared."""
    app, ids = seeded_app

    # Override auth for authenticated roles; raise 403 for unauthenticated
    ctx = ROLE_CONTEXTS[role]
    if ctx is not None:
        app.dependency_overrides[_require_auth_context] = lambda: ctx
        app.dependency_overrides[_optional_auth_context] = lambda: ctx
    else:
        async def _no_token():
            raise HTTPException(status_code=403, detail="Not authenticated")
        app.dependency_overrides[_require_auth_context] = _no_token
        app.dependency_overrides[_optional_auth_context] = lambda: None

    url = path.format(**ids)
    body = REQUEST_BODIES.get(f"{method} {path}")

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.request(method, url, json=body)

    if authorized:
        assert response.status_code != 403, (
            f"{method} {url} as {role}: expected authorized, got 403"
        )
    else:
        assert response.status_code == 403, (
            f"{method} {url} as {role}: expected 403, got {response.status_code}"
        )
