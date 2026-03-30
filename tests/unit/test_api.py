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

"""Unit tests for the API"""

from uuid import uuid4

import pytest
from jwcrypto.jwk import JWK

from rs.core.models import BoxRetrievalResults
from rs.ports.inbound.orchestrator import UploadOrchestratorPort
from tests.fixtures import AppFixture, utils

pytestmark = pytest.mark.asyncio


async def test_create_box_requires_data_steward_role(
    auth_jwk: JWK, app_fixture: AppFixture
):
    """Test that POST /boxes rejects unauthenticated requests (401) and regular users
    without the data steward role (403), and accepts data steward requests (201).
    """
    rest_client = app_fixture.rest_client
    core_mock = app_fixture.core_mock

    box_id = uuid4()
    core_mock.upload_orchestrator.create_research_data_upload_box.return_value = box_id

    request_body = {
        "title": "Test Box",
        "description": "A test upload box",
        "storage_alias": "s3-default",
    }

    # Unauthenticated request should be rejected with 401
    response = await rest_client.post("/boxes", json=request_body)
    assert response.status_code == 401

    # Regular user (no data steward role) should be rejected with 403
    regular_user_header = utils.regular_user_auth_header(jwk=auth_jwk)
    response = await rest_client.post(
        "/boxes", json=request_body, headers=regular_user_header
    )
    assert response.status_code == 403

    # Data steward should succeed with 201
    steward_header = utils.data_steward_auth_header(jwk=auth_jwk)
    response = await rest_client.post(
        "/boxes", json=request_body, headers=steward_header
    )
    assert response.status_code == 201
    core_mock.upload_orchestrator.create_research_data_upload_box.assert_awaited_once()


async def test_get_boxes_requires_authentication(
    auth_jwk: JWK, app_fixture: AppFixture
):
    """Test that GET /boxes rejects unauthenticated requests with 401, but allows
    both regular users and data stewards.
    """
    rest_client = app_fixture.rest_client
    core_mock = app_fixture.core_mock

    core_mock.upload_orchestrator.get_research_data_upload_boxes.return_value = (
        BoxRetrievalResults(count=0, boxes=[])
    )

    # Unauthenticated request should be rejected with 401
    response = await rest_client.get("/boxes")
    assert response.status_code == 401

    # Regular user (not a steward) should be allowed — this endpoint uses UserAuthContext
    regular_user_header = utils.regular_user_auth_header(jwk=auth_jwk)
    response = await rest_client.get("/boxes", headers=regular_user_header)
    assert response.status_code == 200

    # Data steward should also be allowed
    steward_header = utils.data_steward_auth_header(jwk=auth_jwk)
    response = await rest_client.get("/boxes", headers=steward_header)
    assert response.status_code == 200


async def test_get_box_returns_404_on_not_found(auth_jwk: JWK, app_fixture: AppFixture):
    """Test that GET /boxes/{box_id} returns 404 when BoxNotFoundError is raised."""
    rest_client = app_fixture.rest_client
    core_mock = app_fixture.core_mock

    missing_box_id = uuid4()
    core_mock.upload_orchestrator.get_research_data_upload_box.side_effect = (
        UploadOrchestratorPort.BoxNotFoundError(box_id=missing_box_id)
    )

    steward_header = utils.data_steward_auth_header(jwk=auth_jwk)
    response = await rest_client.get(f"/boxes/{missing_box_id}", headers=steward_header)
    assert response.status_code == 404


async def test_create_box_returns_500_on_internal_error(
    auth_jwk: JWK, app_fixture: AppFixture
):
    """Test that POST /boxes returns 500 when an unexpected exception is raised."""
    rest_client = app_fixture.rest_client
    core_mock = app_fixture.core_mock

    core_mock.upload_orchestrator.create_research_data_upload_box.side_effect = (
        RuntimeError("unexpected core failure")
    )

    request_body = {
        "title": "Test Box",
        "description": "A test upload box",
        "storage_alias": "s3-default",
    }
    steward_header = utils.data_steward_auth_header(jwk=auth_jwk)
    response = await rest_client.post(
        "/boxes", json=request_body, headers=steward_header
    )
    assert response.status_code == 500
