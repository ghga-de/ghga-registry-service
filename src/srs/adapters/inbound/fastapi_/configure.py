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

"""FastAPI app configuration for the Study Registry Service."""

from typing import Any

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from ghga_service_commons.api import ApiConfigBase, configure_app

from srs import __version__
from srs.adapters.inbound.fastapi_.data_access_routes import data_access_router
from srs.adapters.inbound.fastapi_.routes import router


def _custom_openapi(app: FastAPI) -> dict[str, Any]:
    """Generate a customised OpenAPI schema."""
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Study Registry Service",
        version=__version__,
        description="REST API for the GHGA Study Registry Service",
        routes=app.routes,
    )
    app.openapi_schema = openapi_schema
    return openapi_schema


def get_configured_app(*, config: ApiConfigBase) -> FastAPI:
    """Create, configure and return the FastAPI application."""
    app = FastAPI()
    app.include_router(router)
    app.include_router(data_access_router)
    configure_app(app, config=config)
    app.openapi = lambda: _custom_openapi(app)  # type: ignore[assignment]
    return app
