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

"""JWT-based HTTP authorization for the Study Registry Service."""

from typing import Annotated
from uuid import UUID

from fastapi import Depends, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from ghga_service_commons.auth.ghga import AuthContext, JWTAuthContextProvider
from pydantic import BaseModel, ConfigDict

from srs.adapters.inbound.fastapi_ import dummies


class AuthProviderConfig(BaseModel):
    """Configuration needed to set up the auth provider."""

    model_config = ConfigDict(frozen=True)


class AuthProviderBundle:
    """Bundle of auth providers for the Study Registry Service."""

    def __init__(self, *, context_provider: JWTAuthContextProvider[AuthContext]):
        self.context_provider = context_provider


async def _require_auth_context(
    bundle: Annotated[AuthProviderBundle, Depends(dummies.auth_provider)],
    credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer(auto_error=True)),
) -> AuthContext:
    """Extract and validate the auth context from the bearer token."""
    return await bundle.context_provider.require_auth_context_using_credentials(
        credentials
    )


require_auth_context = Security(_require_auth_context)

AuthContextDep = Annotated[AuthContext, require_auth_context]


def get_user_id(auth_context: AuthContext) -> UUID:
    """Extract the user ID from an auth context."""
    return UUID(auth_context.id)


def is_data_steward(auth_context: AuthContext) -> bool:
    """Check if the authenticated user has the data_steward role."""
    return "data_steward" in getattr(auth_context, "role", [])
