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

"""Entry-point functions to run the Study Registry Service."""

import asyncio

from ghga_service_commons.api import run_server

from srs.config import Config
from srs.inject import prepare_rest_app


async def run_rest_app() -> None:
    """Run the REST API server."""
    config = Config()  # type: ignore[call-arg]

    async with prepare_rest_app(config=config) as app:
        await run_server(app=app, config=config)
