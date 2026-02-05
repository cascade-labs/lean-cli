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

from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import requests
from requests_aws4auth import AWS4Auth

from lean.components.util.logger import Logger


class _ProgressFileWrapper:
    """File wrapper that tracks upload progress."""

    def __init__(self, file_path: Path, logger: Logger):
        self._file_path = file_path
        self._logger = logger
        self._file = open(file_path, "rb")
        self._size = file_path.stat().st_size
        self._bytes_read = 0
        self._last_percent = 0

    def read(self, size=-1):
        chunk = self._file.read(size)
        if chunk:
            self._bytes_read += len(chunk)
            percent = int((self._bytes_read / self._size) * 100)
            if percent > self._last_percent and percent % 10 == 0:
                self._logger.info(f"Upload progress: {percent}%")
                self._last_percent = percent
        return chunk

    def __len__(self):
        return self._size

    def close(self):
        self._file.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class S3StorageClient:
    """Client for OCI Object Storage access via S3-compatible API."""

    # Object paths
    CONTAINER_KEY_TEMPLATE = "lean/containers/{container_type}/latest.tar.gz"
    CLI_KEY = "lean/cli/latest.tar.gz"

    def __init__(
        self,
        logger: Logger,
        endpoint: str,
        bucket: str,
        access_key: str,
        secret_key: str,
        region: str = "us-ashburn-1"
    ) -> None:
        """Creates a new S3StorageClient instance.

        :param logger: the logger to use for debug messages
        :param endpoint: the S3-compatible endpoint URL (used to extract namespace)
        :param bucket: the bucket name
        :param access_key: the S3 access key
        :param secret_key: the S3 secret key
        :param region: the region (default: us-ashburn-1)
        """
        self._logger = logger
        self._bucket = bucket
        self._region = region
        self._access_key = access_key
        self._secret_key = secret_key

        # Extract namespace from endpoint
        # Endpoint format: <namespace>.compat.objectstorage.<region>.oci.customer-oci.com
        # or: <namespace>.private.objectstorage.<region>.oci.customer-oci.com
        # or: <namespace>.objectstorage.<region>.oraclecloud.com
        if not endpoint.startswith("http"):
            endpoint = f"https://{endpoint}"

        # Parse namespace from endpoint hostname
        parsed = urlparse(endpoint)
        hostname = parsed.netloc or parsed.path
        self._namespace = hostname.split(".")[0]

        # Build S3-compatible endpoint URL (public endpoint)
        # OCI S3 compatibility endpoint format: https://<namespace>.compat.objectstorage.<region>.oraclecloud.com
        self._s3_endpoint = f"https://{self._namespace}.compat.objectstorage.{region}.oraclecloud.com"

        # Create AWS4Auth for S3 signature
        self._auth = AWS4Auth(access_key, secret_key, region, "s3")

    def _get_container_key(self, container_type: str) -> str:
        """Get the object key for a container type."""
        return self.CONTAINER_KEY_TEMPLATE.format(container_type=container_type)

    def upload_container(
        self,
        container_type: str,
        file_path: Path,
        content_hash: str,
        image_name: str,
        image_tag: str = "latest"
    ) -> Dict[str, Any]:
        """Upload a container tarball to OCI Object Storage.

        :param container_type: container type (engine or research)
        :param file_path: path to the compressed tarball file
        :param content_hash: SHA256 hash of the tarball
        :param image_name: Docker image name
        :param image_tag: Docker image tag (default: latest)
        :return: upload result metadata
        """
        key = self._get_container_key(container_type)
        file_size = file_path.stat().st_size

        self._logger.info(f"Uploading {file_size / (1024*1024):.1f} MB to OCI Object Storage...")

        # Store metadata as object metadata (x-amz-meta-* headers)
        metadata = {
            "content-hash": content_hash,
            "image-name": image_name,
            "image-tag": image_tag,
            "container-type": container_type,
        }

        url = f"{self._s3_endpoint}/{self._bucket}/{key}"

        # Build headers with metadata
        headers = {
            "Content-Type": "application/gzip",
            "Content-Length": str(file_size),
        }
        for k, v in metadata.items():
            headers[f"x-amz-meta-{k}"] = v

        # Upload with progress tracking using file wrapper
        with _ProgressFileWrapper(file_path, self._logger) as file_wrapper:
            response = requests.put(
                url,
                data=file_wrapper,
                headers=headers,
                auth=self._auth,
                timeout=7200,
            )

        if response.status_code >= 300:
            raise RuntimeError(f"Upload failed: {response.status_code} {response.text}")

        self._logger.info("Upload complete")

        return {
            "id": f"{container_type}-container",
            "content_hash": content_hash,
            "compressed_size_bytes": file_size,
            "image_name": image_name,
            "image_tag": image_tag,
        }

    def get_container_metadata(self, container_type: str) -> Optional[Dict[str, Any]]:
        """Get container metadata from object metadata.

        :param container_type: container type (engine or research)
        :return: metadata dict or None if not found
        """
        key = self._get_container_key(container_type)
        url = f"{self._s3_endpoint}/{self._bucket}/{key}"

        try:
            response = requests.head(url, auth=self._auth, timeout=30)

            if response.status_code == 404:
                return None

            if response.status_code >= 300:
                raise RuntimeError(f"Failed to get metadata: {response.status_code}")

            # Extract metadata from x-amz-meta-* headers
            metadata = {}
            for header, value in response.headers.items():
                if header.lower().startswith("x-amz-meta-"):
                    key_name = header[11:]  # Remove "x-amz-meta-" prefix
                    metadata[key_name] = value

            return {
                "content_hash": metadata.get("content-hash"),
                "image_name": metadata.get("image-name"),
                "image_tag": metadata.get("image-tag"),
                "container_type": metadata.get("container-type", container_type),
                "compressed_size_bytes": int(response.headers.get("Content-Length", 0)),
            }
        except requests.exceptions.RequestException as e:
            if "404" in str(e):
                return None
            raise

    def get_container_hash(self, container_type: str) -> Optional[str]:
        """Get just the content hash for a container.

        :param container_type: container type (engine or research)
        :return: content hash or None if not found
        """
        metadata = self.get_container_metadata(container_type)
        return metadata.get("content_hash") if metadata else None

    def download_container(
        self,
        container_type: str,
        output_path: Path
    ) -> Dict[str, Any]:
        """Download a container tarball from OCI Object Storage.

        :param container_type: container type (engine or research)
        :param output_path: path to write the tarball to
        :return: download metadata
        """
        key = self._get_container_key(container_type)

        # Get metadata first
        metadata = self.get_container_metadata(container_type)
        if metadata is None:
            raise RuntimeError(f"Container '{container_type}' not found in storage")

        expected_size = metadata.get("compressed_size_bytes")
        self._logger.info(f"Downloading container from OCI Object Storage...")
        if expected_size:
            self._logger.info(f"Expected size: {expected_size / (1024*1024):.1f} MB")

        url = f"{self._s3_endpoint}/{self._bucket}/{key}"

        # Stream download with progress tracking
        bytes_downloaded = [0]
        last_percent = [0]

        with requests.get(url, auth=self._auth, stream=True, timeout=7200) as response:
            if response.status_code >= 300:
                raise RuntimeError(f"Download failed: {response.status_code} {response.text}")

            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        bytes_downloaded[0] += len(chunk)
                        if expected_size:
                            percent = int((bytes_downloaded[0] / expected_size) * 100)
                            if percent > last_percent[0] and percent % 10 == 0:
                                self._logger.info(f"Download progress: {percent}%")
                                last_percent[0] = percent

        actual_size = output_path.stat().st_size
        self._logger.info(f"Downloaded {actual_size / (1024*1024):.1f} MB to {output_path}")

        return {
            "content_hash": metadata.get("content_hash"),
            "compressed_size_bytes": actual_size,
            "image_name": metadata.get("image_name"),
            "image_tag": metadata.get("image_tag"),
        }

    # CLI release methods

    def upload_cli(
        self,
        file_path: Path,
        content_hash: str,
        version: str
    ) -> Dict[str, Any]:
        """Upload a CLI release tarball to OCI Object Storage.

        :param file_path: path to the compressed tarball file
        :param content_hash: SHA256 hash of the tarball
        :param version: CLI version string
        :return: upload result metadata
        """
        file_size = file_path.stat().st_size

        self._logger.info(f"Uploading CLI {file_size / (1024*1024):.1f} MB to OCI Object Storage...")

        metadata = {
            "content-hash": content_hash,
            "version": version,
        }

        url = f"{self._s3_endpoint}/{self._bucket}/{self.CLI_KEY}"

        headers = {
            "Content-Type": "application/gzip",
            "Content-Length": str(file_size),
        }
        for k, v in metadata.items():
            headers[f"x-amz-meta-{k}"] = v

        # Upload with progress tracking using file wrapper
        with _ProgressFileWrapper(file_path, self._logger) as file_wrapper:
            response = requests.put(
                url,
                data=file_wrapper,
                headers=headers,
                auth=self._auth,
                timeout=7200,
            )

        if response.status_code >= 300:
            raise RuntimeError(f"Upload failed: {response.status_code} {response.text}")

        self._logger.info("Upload complete")

        return {
            "id": "cli-release",
            "content_hash": content_hash,
            "compressed_size_bytes": file_size,
            "version": version,
        }

    def get_cli_metadata(self) -> Optional[Dict[str, Any]]:
        """Get CLI release metadata from object metadata.

        :return: metadata dict or None if not found
        """
        url = f"{self._s3_endpoint}/{self._bucket}/{self.CLI_KEY}"

        try:
            response = requests.head(url, auth=self._auth, timeout=30)

            if response.status_code == 404:
                return None

            if response.status_code >= 300:
                raise RuntimeError(f"Failed to get metadata: {response.status_code}")

            # Extract metadata from x-amz-meta-* headers
            metadata = {}
            for header, value in response.headers.items():
                if header.lower().startswith("x-amz-meta-"):
                    key_name = header[11:]
                    metadata[key_name] = value

            return {
                "content_hash": metadata.get("content-hash"),
                "version": metadata.get("version"),
                "compressed_size_bytes": int(response.headers.get("Content-Length", 0)),
            }
        except requests.exceptions.RequestException as e:
            if "404" in str(e):
                return None
            raise

    def get_cli_hash(self) -> Optional[str]:
        """Get just the content hash for CLI release.

        :return: content hash or None if not found
        """
        metadata = self.get_cli_metadata()
        return metadata.get("content_hash") if metadata else None

    def download_cli(self, output_path: Path) -> Dict[str, Any]:
        """Download a CLI release tarball from OCI Object Storage.

        :param output_path: path to write the tarball to
        :return: download metadata
        """
        metadata = self.get_cli_metadata()
        if metadata is None:
            raise RuntimeError("CLI release not found in storage")

        expected_size = metadata.get("compressed_size_bytes")
        self._logger.info("Downloading CLI from OCI Object Storage...")
        if expected_size:
            self._logger.info(f"Expected size: {expected_size / (1024*1024):.1f} MB")

        url = f"{self._s3_endpoint}/{self._bucket}/{self.CLI_KEY}"

        bytes_downloaded = [0]
        last_percent = [0]

        with requests.get(url, auth=self._auth, stream=True, timeout=7200) as response:
            if response.status_code >= 300:
                raise RuntimeError(f"Download failed: {response.status_code} {response.text}")

            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        bytes_downloaded[0] += len(chunk)
                        if expected_size:
                            percent = int((bytes_downloaded[0] / expected_size) * 100)
                            if percent > last_percent[0] and percent % 10 == 0:
                                self._logger.info(f"Download progress: {percent}%")
                                last_percent[0] = percent

        actual_size = output_path.stat().st_size
        self._logger.info(f"Downloaded {actual_size / (1024*1024):.1f} MB to {output_path}")

        return {
            "content_hash": metadata.get("content_hash"),
            "compressed_size_bytes": actual_size,
            "version": metadata.get("version"),
        }
