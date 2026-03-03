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

"""Helper dependencies for requiring authentication and authorization."""

from functools import partial
from typing import Annotated
from uuid import UUID

from fastapi import Depends, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from ghga_service_commons.auth.ghga import AuthContext, has_role
from ghga_service_commons.auth.policies import (
    require_auth_context_using_credentials,
)

from srs.adapters.inbound.fastapi_ import dummies

__all__ = ["OptionalAuthContext", "StewardAuthContext", "UserAuthContext"]


# ── Required auth (any authenticated user) ──────────────────────


async def _require_auth_context(
    credentials: Annotated[
        HTTPAuthorizationCredentials, Depends(HTTPBearer(auto_error=True))
    ],
    auth_provider: dummies.AuthProviderDummy,
) -> AuthContext:
    """Require a GHGA auth context using FastAPI."""
    return await require_auth_context_using_credentials(
        credentials, auth_provider
    )


# ── Steward auth (requires data_steward role) ───────────────────

is_steward = partial(has_role, role="data_steward")


async def _require_steward_context(
    credentials: Annotated[
        HTTPAuthorizationCredentials, Depends(HTTPBearer(auto_error=True))
    ],
    auth_provider: dummies.AuthProviderDummy,
) -> AuthContext:
    """Require a GHGA auth context with data steward role."""
    return await require_auth_context_using_credentials(
        credentials, auth_provider, is_steward
    )


# ── Optional auth (returns AuthContext | None) ──────────────────


async def _optional_auth_context(
    credentials: Annotated[
        HTTPAuthorizationCredentials | None,
        Depends(HTTPBearer(auto_error=False)),
    ],
    auth_provider: dummies.AuthProviderDummy,
) -> AuthContext | None:
    """Return auth context if a token was provided, None otherwise."""
    if credentials is None:
        return None
    return await require_auth_context_using_credentials(
        credentials, auth_provider
    )


# ── Typed dependencies for route signatures ─────────────────────

UserAuthContext = Annotated[AuthContext, Security(_require_auth_context)]
StewardAuthContext = Annotated[AuthContext, Security(_require_steward_context)]
OptionalAuthContext = Annotated[
    AuthContext | None, Security(_optional_auth_context)
]


# ── Helpers ─────────────────────────────────────────────────────


def get_user_id(auth_context: AuthContext) -> UUID:
    """Extract the user ID from an auth context."""
    return UUID(auth_context.id)


def get_optional_user_id(auth_context: AuthContext | None) -> UUID | None:
    """Extract the user ID if an auth context is present."""
    return UUID(auth_context.id) if auth_context else None


def is_data_steward(auth_context: AuthContext) -> bool:
    """Check if the auth context has the data_steward role."""
    return has_role(auth_context, role="data_steward")


def is_optional_data_steward(auth_context: AuthContext | None) -> bool:
    """Returns True if auth context is present and has data_steward role."""
    return auth_context is not None and has_role(
        auth_context, role="data_steward"
    )
