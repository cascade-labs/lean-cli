# QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
# Lean CLI v1.0. Copyright 2021 QuantConnect Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from uuid import UUID

from lean.components.util.http_client import HTTPClient
from lean.components.util.logger import Logger
from lean.models.errors import RequestFailedError


class _ProgressFileWrapper:
    """File wrapper that tracks and logs upload progress.

    This is a file-like object that wraps a file and logs progress
    as it's read. It supports the iterator protocol for streaming uploads.
    """

    def __init__(self, file_path: Path, total_size: int, logger: Logger, chunk_size: int = 8 * 1024 * 1024):
        self._file_path = Path(file_path)
        self._total_size = total_size
        self._logger = logger
        self._chunk_size = chunk_size
        self._file = None
        self._bytes_read = 0
        self._last_percent = 0

    def __iter__(self):
        self._file = open(self._file_path, "rb")
        self._bytes_read = 0
        self._last_percent = 0
        return self

    def __next__(self):
        chunk = self._file.read(self._chunk_size)
        if not chunk:
            self._file.close()
            raise StopIteration

        self._bytes_read += len(chunk)
        percent = int((self._bytes_read / self._total_size) * 100)

        if percent > self._last_percent and percent % 10 == 0:
            self._logger.info(f"Upload progress: {percent}%")
            self._last_percent = percent

        return chunk

    def __len__(self):
        return self._total_size


@dataclass
class DataServerFile:
    """Represents a file in a data server project."""
    id: str
    project_id: str
    file_name: str
    storage_path: str
    content_hash: Optional[str]
    modified_at: datetime
    content: Optional[str] = None


@dataclass
class DataServerProject:
    """Represents a project in the data server."""
    id: str
    name: str
    description: str
    algorithm_language: str
    parameters: Dict[str, Any]
    created_at: datetime
    updated_at: datetime
    files: List[DataServerFile] = None

    def __post_init__(self):
        if self.files is None:
            self.files = []


class DataServerClient:
    """Client for interacting with the CascadeLabs data server lean projects API."""

    def __init__(self, logger: Logger, http_client: HTTPClient, base_url: str, api_key: str) -> None:
        """Creates a new DataServerClient instance.

        :param logger: the logger to use for debug messages
        :param http_client: the HTTP client to make requests with
        :param base_url: the base URL of the data server
        :param api_key: the API key for authentication
        """
        self._logger = logger
        self._http_client = http_client
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key

    def _get_headers(self) -> Dict[str, str]:
        """Returns headers for authenticated requests."""
        return {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json"
        }

    def _request(self, method: str, endpoint: str, data: Optional[Dict[str, Any]] = None) -> Any:
        """Makes an authenticated request to the data server.

        :param method: the HTTP method
        :param endpoint: the API endpoint
        :param data: optional JSON data for POST/PUT requests
        :return: the parsed response
        """
        url = f"{self._base_url}/api/v1/lean/projects{endpoint}"

        options = {"headers": self._get_headers()}
        if data is not None:
            options["json"] = data

        response = self._http_client.request(method, url, raise_for_status=False, **options)

        if self._logger.debug_logging_enabled:
            self._logger.debug(f"Data server response: {response.text}")

        if response.status_code < 200 or response.status_code >= 300:
            raise RequestFailedError(response)

        if response.status_code == 204:
            return None

        return response.json()

    def _parse_project(self, data: Dict[str, Any]) -> DataServerProject:
        """Parses a project response into a DataServerProject object."""
        files = []
        if "files" in data:
            for f in data["files"]:
                files.append(DataServerFile(
                    id=f["id"],
                    project_id=f["project_id"],
                    file_name=f["file_name"],
                    storage_path=f["storage_path"],
                    content_hash=f.get("content_hash"),
                    modified_at=datetime.fromisoformat(f["modified_at"].replace("Z", "+00:00")),
                    content=f.get("content")
                ))

        return DataServerProject(
            id=data["id"],
            name=data["name"],
            description=data["description"],
            algorithm_language=data["algorithm_language"],
            parameters=data["parameters"],
            created_at=datetime.fromisoformat(data["created_at"].replace("Z", "+00:00")),
            updated_at=datetime.fromisoformat(data["updated_at"].replace("Z", "+00:00")),
            files=files
        )

    def create_project(self, name: str, files: List[Dict[str, str]],
                       description: str = "", algorithm_language: str = "Python",
                       parameters: Optional[Dict[str, Any]] = None) -> DataServerProject:
        """Creates a new project with files.

        :param name: the project name
        :param files: list of dicts with 'name' and 'content' keys
        :param description: optional project description
        :param algorithm_language: the algorithm language (default: Python)
        :param parameters: optional project parameters
        :return: the created project
        """
        data = {
            "name": name,
            "description": description,
            "algorithm_language": algorithm_language,
            "parameters": parameters or {},
            "files": [{"name": f["name"], "content": f["content"]} for f in files]
        }
        response = self._request("post", "", data)
        return self._parse_project(response)

    def get_project(self, project_id: str) -> DataServerProject:
        """Gets a project by ID with all files.

        :param project_id: the project UUID
        :return: the project with files
        """
        response = self._request("get", f"/{project_id}")
        return self._parse_project(response)

    def get_project_by_name(self, name: str) -> DataServerProject:
        """Gets a project by name with all files.

        :param name: the project name
        :return: the project with files
        """
        response = self._request("get", f"/by-name/{name}")
        return self._parse_project(response)

    def list_projects(self) -> List[DataServerProject]:
        """Lists all projects (metadata only).

        :return: list of projects
        """
        response = self._request("get", "")
        return [self._parse_project(p) for p in response]

    def update_project(self, project_id: str, files: List[Dict[str, str]],
                       description: Optional[str] = None,
                       algorithm_language: Optional[str] = None,
                       parameters: Optional[Dict[str, Any]] = None) -> DataServerProject:
        """Updates a project and syncs files.

        :param project_id: the project UUID
        :param files: list of dicts with 'name' and 'content' keys
        :param description: optional new description
        :param algorithm_language: optional new algorithm language
        :param parameters: optional new parameters
        :return: the updated project
        """
        data = {
            "files": [{"name": f["name"], "content": f["content"]} for f in files]
        }
        if description is not None:
            data["description"] = description
        if algorithm_language is not None:
            data["algorithm_language"] = algorithm_language
        if parameters is not None:
            data["parameters"] = parameters

        response = self._request("put", f"/{project_id}", data)
        return self._parse_project(response)

    def delete_project(self, project_id: str) -> None:
        """Deletes a project and all its files.

        :param project_id: the project UUID to delete
        """
        self._request("delete", f"/{project_id}")

    def is_authenticated(self) -> bool:
        """Checks whether the current credentials are valid.

        :return: True if credentials are valid
        """
        try:
            self.list_projects()
            return True
        except RequestFailedError:
            return False

    # Backtest API methods

    def _backtest_request(self, method: str, endpoint: str, data: Optional[Dict[str, Any]] = None) -> Any:
        """Makes an authenticated request to the backtests API.

        :param method: the HTTP method
        :param endpoint: the API endpoint
        :param data: optional JSON data for POST/PUT/PATCH requests
        :return: the parsed response
        """
        url = f"{self._base_url}/api/v1/backtests{endpoint}"

        options = {"headers": self._get_headers()}
        if data is not None:
            options["json"] = data

        response = self._http_client.request(method, url, raise_for_status=False, **options)

        if self._logger.debug_logging_enabled:
            self._logger.debug(f"Data server response: {response.text}")

        if response.status_code < 200 or response.status_code >= 300:
            raise RequestFailedError(response)

        if response.status_code == 204:
            return None

        return response.json()

    def create_backtest(
        self,
        project_id: str,
        name: str,
        parameters: Optional[Dict[str, Any]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_capital: float = 100000,
        data_provider_historical: Optional[str] = None,
        provider_config: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Creates a new backtest job.

        :param project_id: the project UUID
        :param name: the backtest name
        :param parameters: optional algorithm parameters
        :param start_date: optional start date (ISO format)
        :param end_date: optional end date (ISO format)
        :param initial_capital: initial capital (default 100000)
        :param data_provider_historical: optional historical data provider
        :param provider_config: optional provider config associations (data-provider, data-downloader, etc.)
        :return: the created backtest
        """
        data = {
            "project_id": project_id,
            "name": name,
            "parameters": parameters or {},
            "initial_capital": initial_capital
        }
        if start_date:
            data["start_date"] = start_date
        if end_date:
            data["end_date"] = end_date
        # Store provider config in config dict
        config = {}
        if data_provider_historical:
            config["data_provider_historical"] = data_provider_historical
        if provider_config:
            config.update(provider_config)
        if config:
            data["config"] = config

        return self._backtest_request("post", "", data)

    def get_backtest(self, backtest_id: str) -> Dict[str, Any]:
        """Gets a backtest by ID.

        :param backtest_id: the backtest UUID
        :return: the backtest data
        """
        return self._backtest_request("get", f"/{backtest_id}")

    def list_backtests(
        self,
        project_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Lists backtests with optional filtering.

        :param project_id: optional filter by project
        :param status: optional filter by status
        :param limit: maximum results
        :return: list of backtests
        """
        params = []
        if project_id:
            params.append(f"project_id={project_id}")
        if status:
            params.append(f"status={status}")
        params.append(f"limit={limit}")

        endpoint = "?" + "&".join(params) if params else ""
        return self._backtest_request("get", endpoint)

    def cancel_backtest(self, backtest_id: str) -> Dict[str, Any]:
        """Cancels a pending or running backtest.

        :param backtest_id: the backtest UUID
        :return: the cancelled backtest
        """
        return self._backtest_request("post", f"/{backtest_id}/cancel")

    def get_backtest_report(self, backtest_id: str) -> bytes:
        """Gets the HTML report for a backtest.

        :param backtest_id: the backtest UUID
        :return: the HTML report content
        """
        url = f"{self._base_url}/api/v1/backtests/{backtest_id}/report"
        response = self._http_client.request("get", url, headers=self._get_headers(), raise_for_status=False)

        if response.status_code < 200 or response.status_code >= 300:
            raise RequestFailedError(response)

        return response.content

    def get_backtest_results(self, backtest_id: str) -> Dict[str, Any]:
        """Gets the JSON results for a backtest (for report generation).

        :param backtest_id: the backtest UUID
        :return: the backtest results JSON
        """
        return self._backtest_request("get", f"/{backtest_id}/results")

    def get_backtest_insights(self, backtest_id: str) -> List[Dict[str, Any]]:
        """Gets the alpha insights for a backtest.

        :param backtest_id: the backtest UUID
        :return: list of insights from the Alpha Framework
        """
        url = f"{self._base_url}/api/v1/backtests/{backtest_id}/insights"
        response = self._http_client.request("get", url, headers=self._get_headers(), raise_for_status=False)

        if response.status_code < 200 or response.status_code >= 300:
            raise RequestFailedError(response)

        return response.json()

    def get_latest_backtest(self, status: str = "completed") -> Optional[Dict[str, Any]]:
        """Gets the most recent backtest with the given status.

        :param status: filter by status (default: completed)
        :return: the latest backtest or None if none found
        """
        backtests = self.list_backtests(status=status, limit=1)
        return backtests[0] if backtests else None

    def get_backtest_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Gets a backtest by name.

        :param name: the backtest name
        :return: the backtest or None if not found
        """
        backtests = self.list_backtests(limit=100)
        for bt in backtests:
            if bt.get("name") == name:
                return bt
        return None

    # Config API methods

    def _config_request(self, method: str, endpoint: str, data: Optional[Dict[str, Any]] = None,
                        params: Optional[Dict[str, str]] = None) -> Any:
        """Makes an authenticated request to the lean config API.

        :param method: the HTTP method
        :param endpoint: the API endpoint
        :param data: optional JSON data for POST/PUT/PATCH requests
        :param params: optional query parameters
        :return: the parsed response
        """
        url = f"{self._base_url}/api/v1/lean/config{endpoint}"
        if params:
            query_string = "&".join(f"{k}={v}" for k, v in params.items() if v is not None)
            if query_string:
                url = f"{url}?{query_string}"

        options = {"headers": self._get_headers()}
        if data is not None:
            options["json"] = data

        response = self._http_client.request(method, url, raise_for_status=False, **options)

        if self._logger.debug_logging_enabled:
            self._logger.debug(f"Data server config response: {response.text}")

        if response.status_code < 200 or response.status_code >= 300:
            raise RequestFailedError(response)

        if response.status_code == 204:
            return None

        return response.json()

    def push_config(
        self,
        config: Dict[str, Any],
        name: str = "default",
        project_id: Optional[str] = None,
        description: str = ""
    ) -> Dict[str, Any]:
        """Push (upsert) a lean config to the server.

        If a config with the same project_id and name exists, it will be updated.
        Otherwise, a new config will be created.

        :param config: the configuration data (lean.json contents)
        :param name: the config name (default: "default")
        :param project_id: optional project UUID for project-specific config
        :param description: optional description
        :return: the created/updated config
        """
        data = {
            "name": name,
            "config": config,
            "description": description
        }
        if project_id is not None:
            data["project_id"] = project_id

        return self._config_request("post", "", data)

    def pull_config(
        self,
        name: str = "default",
        project_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Pull a lean config from the server.

        If project_id is provided, tries to find a project-specific config first.
        If not found, falls back to the global config with the same name.

        :param name: the config name (default: "default")
        :param project_id: optional project UUID
        :return: the config data
        """
        params = {"name": name}
        if project_id is not None:
            params["project_id"] = project_id

        return self._config_request("get", "", params=params)

    def list_configs(self, project_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Lists all configs, optionally filtered by project.

        :param project_id: optional project UUID to filter by
        :return: list of configs
        """
        params = {}
        if project_id is not None:
            params["project_id"] = project_id

        return self._config_request("get", "/list", params=params if params else None)

    # Container API methods

    def _container_request(self, method: str, endpoint: str) -> Any:
        """Makes an authenticated request to the lean containers API.

        :param method: the HTTP method
        :param endpoint: the API endpoint
        :return: the parsed response
        """
        url = f"{self._base_url}/api/v1/lean/containers{endpoint}"

        options = {"headers": self._get_headers()}

        response = self._http_client.request(method, url, raise_for_status=False, **options)

        if self._logger.debug_logging_enabled:
            self._logger.debug(f"Data server container response: {response.text}")

        if response.status_code < 200 or response.status_code >= 300:
            raise RequestFailedError(response)

        if response.status_code == 204:
            return None

        return response.json()

    def push_container(
        self,
        container_type: str,
        tarball: bytes,
        image_name: str,
        image_tag: str = "latest"
    ) -> Dict[str, Any]:
        """Push (upload/replace) a container tarball.

        :param container_type: container type (engine or research)
        :param tarball: compressed tarball bytes
        :param image_name: Docker image name
        :param image_tag: Docker image tag (default: latest)
        :return: the created/updated container metadata
        """
        url = f"{self._base_url}/api/v1/lean/containers"
        headers = {"Authorization": f"Bearer {self._api_key}"}

        files = {"tarball": ("container.tar.gz", tarball, "application/gzip")}
        data = {
            "container_type": container_type,
            "image_name": image_name,
            "image_tag": image_tag
        }

        response = self._http_client.request(
            "post", url, headers=headers, files=files, data=data, raise_for_status=False
        )

        if self._logger.debug_logging_enabled:
            self._logger.debug(f"Data server container push response: {response.text}")

        if response.status_code < 200 or response.status_code >= 300:
            raise RequestFailedError(response)

        return response.json()

    def get_container_upload_url(self, container_type: str) -> Dict[str, Any]:
        """Get a presigned URL for direct S3 upload.

        :param container_type: container type (engine or research)
        :return: dict with upload_url and storage_path
        """
        return self._container_request("get", f"/{container_type}/upload-url")

    def confirm_container_upload(
        self,
        container_type: str,
        content_hash: str,
        image_name: str,
        image_tag: str = "latest",
        compressed_size_bytes: Optional[int] = None
    ) -> Dict[str, Any]:
        """Confirm a direct S3 upload and register the container.

        :param container_type: container type (engine or research)
        :param content_hash: SHA256 hash of the uploaded tarball
        :param image_name: Docker image name
        :param image_tag: Docker image tag (default: latest)
        :param compressed_size_bytes: size of the uploaded file
        :return: the created/updated container metadata
        """
        url = f"{self._base_url}/api/v1/lean/containers/{container_type}/confirm"
        headers = {"Authorization": f"Bearer {self._api_key}"}

        data = {
            "content_hash": content_hash,
            "image_name": image_name,
            "image_tag": image_tag,
        }
        if compressed_size_bytes is not None:
            data["compressed_size_bytes"] = str(compressed_size_bytes)

        response = self._http_client.request(
            "post", url, headers=headers, data=data, raise_for_status=False
        )

        if self._logger.debug_logging_enabled:
            self._logger.debug(f"Data server container confirm response: {response.text}")

        if response.status_code < 200 or response.status_code >= 300:
            raise RequestFailedError(response)

        return response.json()

    def push_container_file(
        self,
        container_type: str,
        tarball_path,
        content_hash: str,
        image_name: str,
        image_tag: str = "latest"
    ) -> Dict[str, Any]:
        """Push (upload/replace) a container tarball using direct S3 upload.

        This method uploads directly to S3 using a presigned URL, bypassing
        the data server for the actual file transfer. This is much more
        efficient for large files (multi-GB containers).

        Flow:
        1. Get presigned upload URL from data server
        2. Upload directly to S3 using PUT request
        3. Confirm upload with data server

        :param container_type: container type (engine or research)
        :param tarball_path: path to the compressed tarball file
        :param content_hash: SHA256 hash of the tarball
        :param image_name: Docker image name
        :param image_tag: Docker image tag (default: latest)
        :return: the created/updated container metadata
        """
        import requests

        tarball_path = Path(tarball_path)
        file_size = tarball_path.stat().st_size

        # Step 1: Get presigned upload URL
        self._logger.info("Getting presigned upload URL...")
        url_response = self.get_container_upload_url(container_type)
        upload_url = url_response["upload_url"]

        # Step 2: Upload directly to S3
        self._logger.info(f"Uploading {file_size / (1024*1024):.1f} MB directly to S3...")

        # Create a file wrapper that tracks progress
        file_wrapper = _ProgressFileWrapper(tarball_path, file_size, self._logger)

        response = requests.put(
            upload_url,
            data=file_wrapper,
            headers={
                "Content-Type": "application/gzip",
                "Content-Length": str(file_size),
            },
            timeout=7200,  # 2 hour timeout for large files
        )

        if response.status_code < 200 or response.status_code >= 300:
            raise RuntimeError(f"S3 upload failed: {response.status_code} {response.text}")

        self._logger.info("S3 upload complete")

        # Step 3: Confirm upload with data server
        self._logger.info("Confirming upload with data server...")
        return self.confirm_container_upload(
            container_type=container_type,
            content_hash=content_hash,
            image_name=image_name,
            image_tag=image_tag,
            compressed_size_bytes=file_size,
        )

    def get_container_download_url(self, container_type: str) -> Dict[str, Any]:
        """Get a presigned URL for direct S3 download.

        :param container_type: container type (engine or research)
        :return: dict with download_url and container metadata
        """
        return self._container_request("get", f"/{container_type}/download-url")

    def get_container(self, container_type: str) -> Dict[str, Any]:
        """Get container metadata by type.

        :param container_type: container type (engine or research)
        :return: container metadata
        """
        return self._container_request("get", f"/{container_type}")

    def get_container_hash(self, container_type: str) -> Dict[str, Any]:
        """Get container hash only (lightweight check).

        :param container_type: container type (engine or research)
        :return: container hash response
        """
        return self._container_request("get", f"/{container_type}/hash")

    def download_container(self, container_type: str) -> bytes:
        """Download container tarball directly from S3.

        Uses presigned URL to download directly from S3 for efficiency.

        :param container_type: container type (engine or research)
        :return: tarball bytes
        """
        import requests

        # Get presigned download URL
        url_response = self.get_container_download_url(container_type)
        download_url = url_response["download_url"]

        # Download directly from S3
        response = requests.get(download_url, timeout=7200)

        if response.status_code < 200 or response.status_code >= 300:
            raise RuntimeError(f"S3 download failed: {response.status_code}")

        return response.content

    def download_container_to_file(self, container_type: str, output_path) -> Dict[str, Any]:
        """Download container tarball directly from S3 to a file.

        Uses presigned URL and streaming download to avoid memory issues
        with large files.

        :param container_type: container type (engine or research)
        :param output_path: path to write the tarball to
        :return: dict with download metadata (content_hash, size, etc.)
        """
        import requests

        output_path = Path(output_path)

        # Get presigned download URL and metadata
        url_response = self.get_container_download_url(container_type)
        download_url = url_response["download_url"]
        expected_size = url_response.get("compressed_size_bytes")

        self._logger.info(f"Downloading container from S3...")
        if expected_size:
            self._logger.info(f"Expected size: {expected_size / (1024*1024):.1f} MB")

        # Stream download to file
        with requests.get(download_url, stream=True, timeout=7200) as response:
            response.raise_for_status()

            bytes_written = 0
            last_percent = 0

            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        bytes_written += len(chunk)

                        if expected_size:
                            percent = int((bytes_written / expected_size) * 100)
                            if percent > last_percent and percent % 10 == 0:
                                self._logger.info(f"Download progress: {percent}%")
                                last_percent = percent

        self._logger.info(f"Downloaded {bytes_written / (1024*1024):.1f} MB to {output_path}")

        return {
            "content_hash": url_response.get("content_hash"),
            "compressed_size_bytes": bytes_written,
            "image_name": url_response.get("image_name"),
            "image_tag": url_response.get("image_tag"),
        }

    # CLI Release API methods

    def _cli_request(self, method: str, endpoint: str = "") -> Any:
        """Makes an authenticated request to the lean CLI release API.

        :param method: the HTTP method
        :param endpoint: the API endpoint
        :return: the parsed response
        """
        url = f"{self._base_url}/api/v1/lean/cli{endpoint}"

        options = {"headers": self._get_headers()}

        response = self._http_client.request(method, url, raise_for_status=False, **options)

        if self._logger.debug_logging_enabled:
            self._logger.debug(f"Data server CLI response: {response.text}")

        if response.status_code < 200 or response.status_code >= 300:
            raise RequestFailedError(response)

        if response.status_code == 204:
            return None

        return response.json()

    def push_cli(self, tarball: bytes) -> Dict[str, Any]:
        """Push (upload/replace) a CLI release tarball.

        :param tarball: compressed tarball bytes
        :return: the created/updated CLI release metadata
        """
        url = f"{self._base_url}/api/v1/lean/cli"
        headers = {"Authorization": f"Bearer {self._api_key}"}

        files = {"tarball": ("lean-cli.tar.gz", tarball, "application/gzip")}

        response = self._http_client.request(
            "post", url, headers=headers, files=files, raise_for_status=False
        )

        if self._logger.debug_logging_enabled:
            self._logger.debug(f"Data server CLI push response: {response.text}")

        if response.status_code < 200 or response.status_code >= 300:
            raise RequestFailedError(response)

        return response.json()

    def get_cli(self) -> Dict[str, Any]:
        """Get CLI release metadata.

        :return: CLI release metadata
        """
        return self._cli_request("get")

    def get_cli_hash(self) -> Dict[str, Any]:
        """Get CLI release hash only (lightweight check).

        :return: CLI release hash response
        """
        return self._cli_request("get", "/hash")

    def download_cli(self) -> bytes:
        """Download CLI release tarball.

        :return: tarball bytes
        """
        url = f"{self._base_url}/api/v1/lean/cli/download"
        headers = {"Authorization": f"Bearer {self._api_key}"}

        response = self._http_client.request("get", url, headers=headers, raise_for_status=False)

        if response.status_code < 200 or response.status_code >= 300:
            raise RequestFailedError(response)

        return response.content
