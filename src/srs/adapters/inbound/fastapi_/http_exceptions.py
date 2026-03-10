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

"""HTTP exception classes for the Study Registry Service."""

from ghga_service_commons.httpyexpect.server.exceptions import HttpCustomExceptionBase

class HttpStudyNotFoundError(HttpCustomExceptionBase):
    """Raised when a study is not found."""

    exception_id = "studyNotFound"

    class DataModel(HttpCustomExceptionBase.DataModel):
        study_id: str

    def __init__(self, *, study_id: str):
        super().__init__(
            status_code=404,
            description=f"Study {study_id!r} not found.",
            data={"study_id": study_id},
        )


class HttpPublicationNotFoundError(HttpCustomExceptionBase):
    """Raised when a publication is not found."""

    exception_id = "publicationNotFound"

    class DataModel(HttpCustomExceptionBase.DataModel):
        publication_id: str

    def __init__(self, *, publication_id: str):
        super().__init__(
            status_code=404,
            description=f"Publication {publication_id!r} not found.",
            data={"publication_id": publication_id},
        )


class HttpDatasetNotFoundError(HttpCustomExceptionBase):
    """Raised when a dataset is not found."""

    exception_id = "datasetNotFound"

    class DataModel(HttpCustomExceptionBase.DataModel):
        dataset_id: str

    def __init__(self, *, dataset_id: str):
        super().__init__(
            status_code=404,
            description=f"Dataset {dataset_id!r} not found.",
            data={"dataset_id": dataset_id},
        )


class HttpDacNotFoundError(HttpCustomExceptionBase):
    """Raised when a DAC is not found."""

    exception_id = "dacNotFound"

    class DataModel(HttpCustomExceptionBase.DataModel):
        dac_id: str

    def __init__(self, *, dac_id: str):
        super().__init__(
            status_code=404,
            description=f"DAC {dac_id!r} not found.",
            data={"dac_id": dac_id},
        )


class HttpDapNotFoundError(HttpCustomExceptionBase):
    """Raised when a DAP is not found."""

    exception_id = "dapNotFound"

    class DataModel(HttpCustomExceptionBase.DataModel):
        dap_id: str

    def __init__(self, *, dap_id: str):
        super().__init__(
            status_code=404,
            description=f"DAP {dap_id!r} not found.",
            data={"dap_id": dap_id},
        )


class HttpResourceTypeNotFoundError(HttpCustomExceptionBase):
    """Raised when a resource type is not found."""

    exception_id = "resourceTypeNotFound"

    def __init__(self):
        super().__init__(
            status_code=404,
            description="Resource type not found.",
            data={},
        )


class HttpMetadataNotFoundError(HttpCustomExceptionBase):
    """Raised when experimental metadata is not found."""

    exception_id = "metadataNotFound"

    class DataModel(HttpCustomExceptionBase.DataModel):
        study_id: str

    def __init__(self, *, study_id: str):
        super().__init__(
            status_code=404,
            description=f"Experimental metadata for study {study_id!r} not found.",
            data={"study_id": study_id},
        )


class HttpAccessionNotFoundError(HttpCustomExceptionBase):
    """Raised when an accession is not found."""

    exception_id = "accessionNotFound"

    class DataModel(HttpCustomExceptionBase.DataModel):
        accession_id: str

    def __init__(self, *, accession_id: str):
        super().__init__(
            status_code=404,
            description=f"Accession {accession_id!r} not found.",
            data={"accession_id": accession_id},
        )


class HttpStatusConflictError(HttpCustomExceptionBase):
    """Raised when an operation conflicts with current status."""

    exception_id = "statusConflict"

    class DataModel(HttpCustomExceptionBase.DataModel):
        detail: str

    def __init__(self, *, detail: str):
        super().__init__(
            status_code=409,
            description=detail,
            data={"detail": detail},
        )


class HttpValidationError(HttpCustomExceptionBase):
    """Raised when domain validation fails."""

    exception_id = "validationError"

    class DataModel(HttpCustomExceptionBase.DataModel):
        detail: str

    def __init__(self, *, detail: str):
        super().__init__(
            status_code=422,
            description=detail,
            data={"detail": detail},
        )


class HttpReferenceConflictError(HttpCustomExceptionBase):
    """Raised when deletion is blocked by referencing entities."""

    exception_id = "referenceConflict"

    class DataModel(HttpCustomExceptionBase.DataModel):
        detail: str

    def __init__(self, *, detail: str):
        super().__init__(
            status_code=409,
            description=detail,
            data={"detail": detail},
        )


class HttpDuplicateError(HttpCustomExceptionBase):
    """Raised when a duplicate entity is detected."""

    exception_id = "duplicateEntity"

    class DataModel(HttpCustomExceptionBase.DataModel):
        detail: str

    def __init__(self, *, detail: str):
        super().__init__(
            status_code=409,
            description=detail,
            data={"detail": detail},
        )


class HttpNotAuthorizedError(HttpCustomExceptionBase):
    """Raised when the caller is not authorized."""

    exception_id = "notAuthorized"

    def __init__(self):
        super().__init__(
            status_code=403,
            description="Not authorized.",
            data={},
        )


class HttpInternalError(HttpCustomExceptionBase):
    """Raised for unhandled internal errors."""

    exception_id = "internalError"

    def __init__(self):
        super().__init__(
            status_code=500,
            description="Internal server error.",
            data={},
        )
