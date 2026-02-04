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

# Cache directory for container hashes
CONTAINER_CACHE_DIR = Path.home() / ".lean" / "container-cache"


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


def _get_cached_hash(container_type: str) -> str | None:
    """Get the cached hash for a container type."""
    hash_file = CONTAINER_CACHE_DIR / f"{container_type}.hash"
    if hash_file.exists():
        return hash_file.read_text().strip()
    return None


def _set_cached_hash(container_type: str, content_hash: str) -> None:
    """Set the cached hash for a container type."""
    CONTAINER_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    hash_file = CONTAINER_CACHE_DIR / f"{container_type}.hash"
    hash_file.write_text(content_hash)


@command(cls=LeanCommand)
@option("--type", "container_type", type=Choice(["engine", "research"], case_sensitive=False),
        required=True, help="Container type (engine or research)")
@option("--force", is_flag=True, default=False,
        help="Force pull even if local hash matches remote")
@option("--check-only", is_flag=True, default=False,
        help="Only check if an update is available, don't download")
def pull(container_type: str, force: bool, check_only: bool) -> None:
    """Pull a Docker container image from the data server.

    This downloads a compressed tarball from the data server, decompresses it,
    and loads it into Docker using 'docker load'.

    \b
    Examples:
        lean cloud container pull --type engine
        lean cloud container pull --type research --force
        lean cloud container pull --type engine --check-only
    """
    logger = di_container.logger

    logger.info(f"Checking container '{container_type}' on data server...")

    # Get data server client
    data_server_client = di_container.data_server_client

    # Get remote hash
    try:
        remote_hash_response = data_server_client.get_container_hash(container_type)
        remote_hash = remote_hash_response.get("content_hash")
        logger.info(f"Remote container hash: {remote_hash[:12]}...")
    except RequestFailedError as e:
        if e.response.status_code == 404:
            logger.info(f"No container '{container_type}' found on server")
            return
        raise

    # Check local cached hash
    local_hash = _get_cached_hash(container_type)
    if local_hash:
        logger.info(f"Local cached hash: {local_hash[:12]}...")
    else:
        logger.info("No local cached hash found")

    # Determine if update is needed
    update_needed = force or local_hash != remote_hash

    if not update_needed:
        logger.info("Container is already up to date")
        return

    if check_only:
        logger.info("Update available!")
        logger.info(f"  Local hash:  {local_hash[:12] if local_hash else '(none)'}...")
        logger.info(f"  Remote hash: {remote_hash[:12]}...")
        return

    # Find container runtime early
    runtime = _get_container_runtime()
    runtime_name = Path(runtime).name
    logger.info(f"Using container runtime: {runtime_name}")

    # Download and load into container runtime
    with tempfile.TemporaryDirectory() as tmpdir:
        gz_path = Path(tmpdir) / "container.tar.gz"
        tar_path = Path(tmpdir) / "container.tar"

        # Stream download directly to file (avoids loading entire file into memory)
        logger.info("Downloading container tarball directly from S3...")
        download_info = data_server_client.download_container_to_file(container_type, gz_path)

        # Verify hash by reading the file in chunks
        logger.info("Verifying download integrity...")
        sha256 = hashlib.sha256()
        with open(gz_path, "rb") as f:
            while True:
                chunk = f.read(8192)
                if not chunk:
                    break
                sha256.update(chunk)
        computed_hash = sha256.hexdigest()

        if computed_hash != remote_hash:
            raise RuntimeError(f"Hash mismatch! Expected {remote_hash[:12]}..., got {computed_hash[:12]}...")
        logger.info("Hash verified")

        # Decompress
        logger.info("Decompressing tarball...")
        with gzip.open(gz_path, "rb") as f_in:
            with open(tar_path, "wb") as f_out:
                while True:
                    chunk = f_in.read(8 * 1024 * 1024)  # 8MB chunks
                    if not chunk:
                        break
                    f_out.write(chunk)

        tar_size_mb = tar_path.stat().st_size / (1024 * 1024)
        logger.info(f"Decompressed size: {tar_size_mb:.1f} MB")

        # Load into container runtime
        logger.info(f"Loading image into {runtime_name}...")
        result = subprocess.run(
            [runtime, "load", "-i", str(tar_path)],
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            raise RuntimeError(f"Failed to load image: {result.stderr}")

        # Parse loaded image name from output
        # Output is like "Loaded image: quantconnect/lean:latest"
        for line in result.stdout.strip().split("\n"):
            if "Loaded image" in line:
                logger.info(line)

    # Update cached hash
    _set_cached_hash(container_type, remote_hash)

    logger.info(f"Successfully pulled container '{container_type}'")
