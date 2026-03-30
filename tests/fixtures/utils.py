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

"""Utils for Fixture handling."""

from pathlib import Path
from uuid import UUID

from ghga_service_commons.utils import jwt_helpers
from ghga_service_commons.utils.utc_dates import now_as_utc
from jwcrypto.jwk import JWK

BASE_DIR = Path(__file__).parent.resolve()

TOKEN_LIFESPAN = 30  # seconds
DATA_STEWARD_ID = UUID("6d1a41c3-de07-42f8-80ef-243aa69b6261")
REGULAR_USER_ID = UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")


def _make_ghga_auth_header(
    *,
    user_id: UUID,
    user_name: str,
    user_email: str,
    roles: list[str],
    jwk: JWK,
) -> dict[str, str]:
    """Create a GHGA auth header with a signed JWT containing the given claims."""
    now = now_as_utc()
    claims = {
        "id": str(user_id),
        "name": user_name,
        "email": user_email,
        "iat": int(now.timestamp()),
        "exp": int(now.timestamp()) + TOKEN_LIFESPAN,
        "roles": roles,
    }
    signed_token = jwt_helpers.sign_and_serialize_token(
        claims=claims, key=jwk, valid_seconds=TOKEN_LIFESPAN
    )
    return {"Authorization": f"Bearer {signed_token}"}


def data_steward_auth_header(*, jwk: JWK) -> dict[str, str]:
    """Generate a GHGA auth header for a data steward user."""
    return _make_ghga_auth_header(
        user_id=DATA_STEWARD_ID,
        user_name="Test Data Steward",
        user_email="steward@example.org",
        roles=["data_steward"],
        jwk=jwk,
    )


def regular_user_auth_header(*, jwk: JWK) -> dict[str, str]:
    """Generate a GHGA auth header for a regular (non-steward) user."""
    return _make_ghga_auth_header(
        user_id=REGULAR_USER_ID,
        user_name="Test Regular User",
        user_email="user@example.org",
        roles=[],
        jwk=jwk,
    )
