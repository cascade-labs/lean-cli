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

import shutil
import subprocess
from pathlib import Path

from click import command, option, Choice
from lean.click import LeanCommand
from lean.container import container as di_container


def _get_container_runtime() -> str:
    """Find available container runtime (docker or podman)."""
    docker_path = shutil.which("docker")
    if docker_path:
        return docker_path

    podman_path = shutil.which("podman")
    if podman_path:
        return podman_path

    raise RuntimeError("Neither Docker nor Podman is installed or in PATH")


def _login_to_registry(runtime: str, registry: str, username: str, token: str, logger) -> None:
    """Login to the container registry."""
    logger.info(f"Logging into registry {registry}...")

    result = subprocess.run(
        [runtime, "login", registry, "-u", username, "--password-stdin"],
        input=token.encode(),
        capture_output=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Failed to login to registry: {result.stderr.decode()}")


@command(cls=LeanCommand)
@option("--type", "container_type", type=Choice(["engine", "research"], case_sensitive=False),
        required=True, help="Container type (engine or research)")
@option("--tag", type=str, default=None,
        help="Optional local tag to apply after pulling (e.g., my-lean:latest)")
def pull(container_type: str, tag: str) -> None:
    """Pull a Docker container image from the container registry.

    This pulls the image from the configured container registry using native
    docker/podman pull for efficient layer-based transfer.

    \b
    Examples:
        lean cloud container pull --type engine
        lean cloud container pull --type research --tag my-research:latest
    """
    logger = di_container.logger
    cli_config = di_container.cli_config_manager

    # Get registry config
    registry = cli_config.container_registry.get_value()
    namespace = cli_config.container_registry_namespace.get_value()
    username = cli_config.container_registry_username.get_value()
    token = cli_config.container_registry_token.get_value()

    if not all([registry, namespace, username, token]):
        raise RuntimeError(
            "Container registry not configured. Set the following config values:\n"
            "  lean config set container-registry <registry>\n"
            "  lean config set container-registry-namespace <namespace>\n"
            "  lean config set container-registry-username <username>\n"
            "  lean config set container-registry-token <token>"
        )

    # Find container runtime
    runtime = _get_container_runtime()
    runtime_name = Path(runtime).name
    logger.info(f"Using container runtime: {runtime_name}")

    # Build the remote image name
    remote_image = f"{registry}/{namespace}/lean-{container_type}:latest"

    logger.info(f"Pulling '{remote_image}'")

    # Login to registry
    _login_to_registry(runtime, registry, username, token, logger)

    # Pull from registry
    logger.info(f"Pulling from registry (this uses efficient layer-based transfer)...")
    result = subprocess.run(
        [runtime, "pull", remote_image],
        capture_output=False,  # Show output directly for progress
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to pull image")

    # Optionally tag with a local name
    if tag:
        logger.info(f"Tagging as '{tag}'...")
        result = subprocess.run(
            [runtime, "tag", remote_image, tag],
            capture_output=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Failed to tag image: {result.stderr.decode()}")

    logger.info(f"Successfully pulled container '{container_type}'")
    logger.info(f"  Remote image: {remote_image}")
    if tag:
        logger.info(f"  Local tag: {tag}")
