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

import gzip
import hashlib
import shutil
import subprocess
import tempfile
from pathlib import Path

from click import command, option, Choice
from lean.click import LeanCommand
from lean.container import container as di_container
from lean.models.errors import RequestFailedError


def _get_container_runtime() -> str:
    """Find available container runtime (docker or podman).

    Returns:
        Path to docker or podman executable

    Raises:
        RuntimeError: If neither docker nor podman is installed
    """
    # Try docker first
    docker_path = shutil.which("docker")
    if docker_path:
        return docker_path

    # Fall back to podman
    podman_path = shutil.which("podman")
    if podman_path:
        return podman_path

    raise RuntimeError("Neither Docker nor Podman is installed or in PATH")


@command(cls=LeanCommand)
@option("--type", "container_type", type=Choice(["engine", "research"], case_sensitive=False),
        required=True, help="Container type (engine or research)")
@option("--image", type=str, default=None,
        help="Docker image name:tag (defaults to quantconnect/lean:latest for engine, quantconnect/research:latest for research)")
@option("--force", is_flag=True, default=False,
        help="Force push even if remote hash matches")
def push(container_type: str, image: str, force: bool) -> None:
    """Push a Docker container image to the data server.

    This exports a local Docker image as a tarball, compresses it, and uploads
    it to the data server. Only the latest version is stored (no version history).

    The image must exist locally - use 'docker pull' or 'docker build' first.

    \b
    Examples:
        lean cloud container push --type engine
        lean cloud container push --type engine --image my-custom-lean:v1
        lean cloud container push --type research --force
    """
    logger = di_container.logger

    # Find container runtime
    runtime = _get_container_runtime()
    runtime_name = Path(runtime).name
    logger.info(f"Using container runtime: {runtime_name}")

    # Default image names
    if image is None:
        if container_type == "engine":
            image = "quantconnect/lean:latest"
        else:
            image = "quantconnect/research:latest"

    # Parse image name and tag
    if ":" in image:
        image_name, image_tag = image.rsplit(":", 1)
    else:
        image_name = image
        image_tag = "latest"

    logger.info(f"Pushing container '{container_type}' from image '{image_name}:{image_tag}'")

    # Verify image exists locally
    logger.info("Verifying image exists locally...")
    result = subprocess.run(
        [runtime, "inspect", f"{image_name}:{image_tag}"],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"Image '{image_name}:{image_tag}' not found locally. "
                           f"Use '{runtime_name} pull' or '{runtime_name} build' first.")

    # Get data server client
    data_server_client = di_container.data_server_client

    # Check remote hash to see if we can skip upload
    if not force:
        try:
            remote_hash_response = data_server_client.get_container_hash(container_type)
            remote_hash = remote_hash_response.get("content_hash")
            logger.info(f"Remote container hash: {remote_hash[:12]}...")
        except RequestFailedError as e:
            if e.response.status_code == 404:
                remote_hash = None
                logger.info("No existing container found on server")
            else:
                raise
    else:
        remote_hash = None
        logger.info("Force flag set, skipping remote hash check")

    # Export image to tarball
    with tempfile.TemporaryDirectory() as tmpdir:
        tar_path = Path(tmpdir) / "container.tar"
        gz_path = Path(tmpdir) / "container.tar.gz"

        logger.info(f"Exporting image to tarball...")
        result = subprocess.run(
            [runtime, "save", f"{image_name}:{image_tag}", "-o", str(tar_path)],
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            raise RuntimeError(f"Failed to export image: {result.stderr}")

        tar_size_mb = tar_path.stat().st_size / (1024 * 1024)
        logger.info(f"Exported tarball size: {tar_size_mb:.1f} MB")

        # Compress with gzip
        logger.info("Compressing tarball...")
        with open(tar_path, "rb") as f_in:
            with gzip.open(gz_path, "wb", compresslevel=6) as f_out:
                while True:
                    chunk = f_in.read(8192)
                    if not chunk:
                        break
                    f_out.write(chunk)

        gz_size_mb = gz_path.stat().st_size / (1024 * 1024)
        compression_ratio = (1 - gz_size_mb / tar_size_mb) * 100
        logger.info(f"Compressed size: {gz_size_mb:.1f} MB ({compression_ratio:.1f}% reduction)")

        # Compute SHA256 hash
        logger.info("Computing hash...")
        sha256 = hashlib.sha256()
        with open(gz_path, "rb") as f:
            while True:
                chunk = f.read(8192)
                if not chunk:
                    break
                sha256.update(chunk)
        content_hash = sha256.hexdigest()
        logger.info(f"Content hash: {content_hash[:12]}...")

        # Check if upload is needed
        if remote_hash and remote_hash == content_hash:
            logger.info("Remote container is already up to date, skipping upload")
            return

        # Upload to server (stream from file to avoid memory issues)
        logger.info("Uploading container to server...")
        result = data_server_client.push_container_file(
            container_type=container_type,
            tarball_path=gz_path,
            content_hash=content_hash,
            image_name=image_name,
            image_tag=image_tag
        )

        logger.info(f"Successfully pushed container '{container_type}'")
        logger.info(f"  ID: {result['id']}")
        logger.info(f"  Hash: {result['content_hash'][:12]}...")
        logger.info(f"  Size: {result.get('compressed_size_bytes', 0) / (1024 * 1024):.1f} MB")
