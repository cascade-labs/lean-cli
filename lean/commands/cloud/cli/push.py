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
import io
import tarfile
from pathlib import Path

from click import command, option
from lean.click import LeanCommand
from lean.container import container as di_container
from lean.models.errors import RequestFailedError


def _find_lean_cli_root() -> Path:
    """Find the lean-cli source directory.

    Looks for the lean-cli root by searching for setup.py or pyproject.toml
    starting from the lean package location.
    """
    import lean
    lean_package_path = Path(lean.__file__).parent

    # Go up from lean package to find the repo root
    current = lean_package_path.parent
    for _ in range(5):  # Don't go too far up
        if (current / "pyproject.toml").exists() or (current / "setup.py").exists():
            # Verify this is the lean-cli root by checking for the lean directory
            if (current / "lean").is_dir():
                return current
        current = current.parent

    raise RuntimeError("Could not find lean-cli source directory")


def _create_cli_tarball(source_dir: Path) -> bytes:
    """Create a gzip-compressed tarball of the lean-cli source.

    Args:
        source_dir: Path to the lean-cli root directory

    Returns:
        Compressed tarball bytes
    """
    # Files and directories to include
    include_patterns = [
        "lean",
        "pyproject.toml",
        "setup.py",
        "setup.cfg",
        "README.md",
        "LICENSE",
        "MANIFEST.in",
    ]

    # Files and directories to exclude
    exclude_patterns = [
        "__pycache__",
        ".pyc",
        ".pyo",
        ".git",
        ".pytest_cache",
        ".mypy_cache",
        ".tox",
        ".eggs",
        "*.egg-info",
        ".venv",
        "venv",
        "dist",
        "build",
    ]

    def should_exclude(path: Path) -> bool:
        """Check if a path should be excluded."""
        name = path.name
        for pattern in exclude_patterns:
            if pattern.startswith("*"):
                if name.endswith(pattern[1:]):
                    return True
            elif name == pattern:
                return True
        return False

    # Create tarball in memory
    tar_buffer = io.BytesIO()
    with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
        for pattern in include_patterns:
            path = source_dir / pattern
            if not path.exists():
                continue

            if path.is_file():
                tar.add(path, arcname=pattern)
            elif path.is_dir():
                for file_path in path.rglob("*"):
                    if file_path.is_file() and not should_exclude(file_path):
                        # Check parent directories too
                        skip = False
                        for parent in file_path.relative_to(source_dir).parents:
                            if should_exclude(Path(parent.name)):
                                skip = True
                                break
                        if not skip:
                            arcname = str(file_path.relative_to(source_dir))
                            tar.add(file_path, arcname=arcname)

    # Compress with gzip
    tar_buffer.seek(0)
    gz_buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=gz_buffer, mode="wb", compresslevel=6) as gz:
        gz.write(tar_buffer.read())

    gz_buffer.seek(0)
    return gz_buffer.read()


@command(cls=LeanCommand)
@option("--force", is_flag=True, default=False,
        help="Force push even if remote hash matches")
@option("--source", type=str, default=None,
        help="Path to lean-cli source directory (auto-detected if not specified)")
def push(force: bool, source: str) -> None:
    """Push the lean-cli source to the data server.

    This creates a tarball of the lean-cli source directory and uploads it to
    the data server. Workers can then download and install updates automatically.

    Only the latest version is stored (no version history).

    \b
    Examples:
        lean cloud cli push
        lean cloud cli push --force
        lean cloud cli push --source /path/to/lean-cli
    """
    logger = di_container.logger

    # Find or use provided source directory
    if source:
        source_dir = Path(source)
        if not source_dir.is_dir():
            raise RuntimeError(f"Source directory not found: {source}")
    else:
        source_dir = _find_lean_cli_root()

    logger.info(f"Using lean-cli source from: {source_dir}")

    # Get data server client
    data_server_client = di_container.data_server_client

    # Check remote hash to see if we can skip upload
    if not force:
        try:
            remote_hash_response = data_server_client.get_cli_hash()
            remote_hash = remote_hash_response.get("content_hash")
            logger.info(f"Remote CLI hash: {remote_hash[:12]}...")
        except RequestFailedError as e:
            if e.response.status_code == 404:
                remote_hash = None
                logger.info("No existing CLI release found on server")
            else:
                raise
    else:
        remote_hash = None
        logger.info("Force flag set, skipping remote hash check")

    # Create tarball
    logger.info("Creating tarball of lean-cli source...")
    tarball = _create_cli_tarball(source_dir)
    size_mb = len(tarball) / (1024 * 1024)
    logger.info(f"Tarball size: {size_mb:.2f} MB")

    # Compute hash
    content_hash = hashlib.sha256(tarball).hexdigest()
    logger.info(f"Content hash: {content_hash[:12]}...")

    # Check if upload is needed
    if remote_hash and remote_hash == content_hash:
        logger.info("Remote CLI is already up to date, skipping upload")
        return

    # Upload to server
    logger.info("Uploading CLI to server...")
    result = data_server_client.push_cli(tarball=tarball)

    logger.info("Successfully pushed lean-cli")
    logger.info(f"  ID: {result['id']}")
    logger.info(f"  Hash: {result['content_hash'][:12]}...")
    logger.info(f"  Size: {result.get('compressed_size_bytes', 0) / (1024 * 1024):.2f} MB")
