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
@option("--image", type=str, default=None,
        help="Local Docker image name:tag to push")
def push(container_type: str, image: str) -> None:
    """Push a Docker container image to the container registry.

    This tags and pushes a local Docker image to the configured container registry
    using native docker/podman push for efficient layer-based transfer.

    \b
    Examples:
        lean cloud container push --type engine --image my-lean:latest
        lean cloud container push --type research --image my-research:v1
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

    if not image:
        raise RuntimeError("--image is required. Specify the local image to push.")

    # Find container runtime
    runtime = _get_container_runtime()
    runtime_name = Path(runtime).name
    logger.info(f"Using container runtime: {runtime_name}")

    # Verify image exists locally
    logger.info(f"Verifying image '{image}' exists locally...")
    result = subprocess.run(
        [runtime, "inspect", image],
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Image '{image}' not found locally. "
                           f"Use '{runtime_name} pull' or '{runtime_name} build' first.")

    # Build the remote image name
    # Format: <registry>/<namespace>/lean-<type>:latest
    remote_image = f"{registry}/{namespace}/lean-{container_type}:latest"

    logger.info(f"Pushing '{image}' to '{remote_image}'")

    # Login to registry
    _login_to_registry(runtime, registry, username, token, logger)

    # Tag the image for the remote registry
    logger.info(f"Tagging image...")
    result = subprocess.run(
        [runtime, "tag", image, remote_image],
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to tag image: {result.stderr.decode()}")

    # Push to registry
    logger.info(f"Pushing to registry (this uses efficient layer-based transfer)...")
    result = subprocess.run(
        [runtime, "push", remote_image],
        capture_output=False,  # Show output directly for progress
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to push image")

    logger.info(f"Successfully pushed container '{container_type}'")
    logger.info(f"  Remote image: {remote_image}")
