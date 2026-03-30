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

"""Integration tests for the REST API with real infrastructure components"""

import pytest

from tests.fixtures import utils
from tests.fixtures.joint import JointFixture

pytestmark = pytest.mark.asyncio


async def test_health_endpoint(joint_fixture: JointFixture):
    """Test that the health endpoint responds with 200 OK."""
    response = await joint_fixture.rest_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "OK"}


async def test_unauthenticated_requests_are_rejected(joint_fixture: JointFixture):
    """Test that protected endpoints reject unauthenticated requests with 401."""
    rest_client = joint_fixture.rest_client

    # GET /boxes requires authentication
    response = await rest_client.get("/boxes")
    assert response.status_code == 401

    # POST /boxes requires authentication
    response = await rest_client.post(
        "/boxes",
        json={
            "title": "Test Box",
            "description": "description",
            "storage_alias": "s3-default",
        },
    )
    assert response.status_code == 401

    # GET /access-grants requires authentication
    response = await rest_client.get("/access-grants")
    assert response.status_code == 401


async def test_regular_user_cannot_create_box(joint_fixture: JointFixture):
    """Test that a user without the data steward role cannot create an upload box."""
    regular_user_header = utils.regular_user_auth_header(jwk=joint_fixture.auth_jwk)
    response = await joint_fixture.rest_client.post(
        "/boxes",
        json={
            "title": "Test Box",
            "description": "description",
            "storage_alias": "s3-default",
        },
        headers=regular_user_header,
    )
    assert response.status_code == 403
