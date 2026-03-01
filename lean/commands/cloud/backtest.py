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

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional, Tuple

from click import command, argument, option

from lean.click import LeanCommand, backtest_parameter_option
from lean.container import container
from lean.components.util.data_provider_config import normalize_data_provider_historical, get_cascade_provider_config


def _format_logs(logs: Any) -> str:
    """Converts the logs API response to a plain string."""
    if logs is None:
        return ""
    if isinstance(logs, str):
        return logs
    if isinstance(logs, list):
        return "\n".join(str(line) for line in logs)
    if isinstance(logs, dict):
        content = logs.get("logs") or logs.get("content") or logs.get("data")
        if content is not None:
            return _format_logs(content)
        return json.dumps(logs, indent=4)
    return str(logs)


def _save_backtest_results_locally(project_path: Path, backtest: dict, results: Any, logs: Any) -> Path:
    """Saves cloud backtest results to a local directory mimicking local backtest output.

    Creates:
        PROJECT/backtests/TIMESTAMP/{backtest_id}.json  - full results
        PROJECT/backtests/TIMESTAMP/logs.txt            - algorithm logs
        PROJECT/backtests/TIMESTAMP/config              - backtest metadata

    :param project_path: path to the local project directory
    :param backtest: backtest metadata dict from the API
    :param results: results JSON from the API
    :param logs: logs from the API
    :return: the path to the created backtest directory
    """
    logger = container.logger

    backtest_id = backtest.get("id", "unknown")
    backtest_name = backtest.get("name", "unnamed")
    completed_at = backtest.get("completed_at", "")

    # Use completion time for the directory name so it matches the actual run time
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    if completed_at:
        try:
            dt = datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
            timestamp = dt.astimezone().strftime("%Y-%m-%d_%H-%M-%S")
        except Exception:
            pass

    output_dir = project_path / "backtests" / timestamp
    output_dir.mkdir(parents=True, exist_ok=True)

    # Results JSON – named by backtest ID to match local engine convention
    results_file = output_dir / f"{backtest_id}.json"
    results_file.write_text(json.dumps(results, indent=4) if results else "{}")

    # Logs
    logs_file = output_dir / "logs.txt"
    logs_file.write_text(_format_logs(logs))

    # Metadata config file (read by _list_local_backtests)
    config_data = {
        "id": backtest_id,
        "backtest-name": backtest_name,
        "created_at": backtest.get("created_at", ""),
        "completed_at": completed_at,
        "status": backtest.get("status", ""),
    }
    (output_dir / "config").write_text(json.dumps(config_data, indent=4))

    logger.info(f"Results saved to: {output_dir}")
    return output_dir


def _resolve_project_path(project_name: str) -> Path:
    """Resolves a project name to a local directory path.

    Looks for an existing directory with the project name relative to cwd,
    falling back to creating one there.
    """
    candidate = Path.cwd() / project_name
    return candidate


def _display_backtest_statistics(backtest: dict, results: Any) -> None:
    """Prints a summary of key backtest statistics."""
    logger = container.logger

    # Try results JSON first for full statistics
    stats = {}
    if isinstance(results, dict):
        stats = results.get("Statistics", {})

    # Fall back to whatever the API stores in backtest.result
    if not stats and isinstance(backtest.get("result"), dict):
        stats = backtest["result"]

    if stats:
        logger.info("")
        logger.info("Statistics:")
        for key, value in stats.items():
            logger.info(f"  {key}: {value}")


def _pull_cascade_backtest_results(backtest_id_or_name: str, project_override: Optional[str]) -> None:
    """Pulls results for a completed cloud backtest and saves them locally.

    :param backtest_id_or_name: the backtest UUID or name
    :param project_override: optional local project directory to save results into
    """
    logger = container.logger
    data_server_client = container.data_server_client

    # Look up the backtest
    backtest = None
    try:
        backtest = data_server_client.get_backtest(backtest_id_or_name)
    except Exception:
        pass

    if backtest is None:
        # Try by name
        backtest = data_server_client.get_backtest_by_name(backtest_id_or_name)
        if backtest is None:
            raise RuntimeError(f'No backtest with the given id or name "{backtest_id_or_name}" found.')

    backtest_id = backtest["id"]
    status = backtest.get("status", "")
    name = backtest.get("name", "unnamed")

    logger.info(f"Found backtest '{name}' (id: {backtest_id}, status: {status})")

    if status != "completed":
        raise RuntimeError(f"Backtest is not completed (status: {status}). Only completed backtests can be pulled.")

    # Resolve local project path
    if project_override:
        project_path = _resolve_project_path(project_override)
    else:
        # Look up project name from the data server
        project_id = backtest.get("project_id", "")
        project_name = None
        if project_id:
            try:
                ds_project = data_server_client.get_project(project_id)
                project_name = ds_project.name
            except Exception:
                pass
        project_path = _resolve_project_path(project_name or "cloud_backtests")

    # Download results and logs
    logger.info("Downloading results...")
    results = None
    try:
        results = data_server_client.get_backtest_results(backtest_id)
    except Exception as e:
        logger.warn(f"Could not download results: {e}")

    logger.info("Downloading logs...")
    logs = None
    try:
        logs = data_server_client.get_backtest_logs(backtest_id)
    except Exception as e:
        logger.warn(f"Could not download logs: {e}")

    output_dir = _save_backtest_results_locally(project_path, backtest, results, logs)

    _display_backtest_statistics(backtest, results)

    logger.info("")
    logger.info(f"Backtest results saved to: {output_dir}")


def _list_cascade_backtests(project: Optional[str], status: Optional[str]) -> None:
    """List backtests from Cascade data server.

    :param project: optional project name to filter by
    :param status: optional status to filter by
    """
    logger = container.logger
    data_server_client = container.data_server_client

    project_id = None
    if project:
        # Get project from data server
        try:
            data_server_project = data_server_client.get_project_by_name(project)
            project_id = data_server_project.id
            logger.info(f"Listing backtests for project '{data_server_project.name}'")
        except Exception:
            try:
                data_server_project = data_server_client.get_project(project)
                project_id = data_server_project.id
                logger.info(f"Listing backtests for project '{data_server_project.name}'")
            except Exception:
                raise RuntimeError(f'No project with the given name or id "{project}" found in the data server.')

    backtests = data_server_client.list_backtests(project_id=project_id, status=status)

    if not backtests:
        logger.info("No backtests found.")
        return

    # Build a project_id -> name map to avoid N individual API calls
    project_name_cache: dict = {}
    if project_id and project:
        # We already know the name for this project
        project_name_cache[project_id] = project
    else:
        try:
            all_projects = data_server_client.list_projects()
            for p in all_projects:
                project_name_cache[p.id] = p.name
        except Exception:
            pass

    logger.info(f"Found {len(backtests)} backtest(s):\n")

    for bt in backtests:
        status_str = bt.get("status", "unknown")
        name = bt.get("name", "unnamed")
        bt_id = bt.get("id", "")
        bt_project_id = bt.get("project_id", "")
        created = bt.get("created_at", "")[:19] if bt.get("created_at") else ""
        project_display = project_name_cache.get(bt_project_id, bt_project_id or "unknown")

        # Format status with color hint
        if status_str == "completed":
            status_display = "✓ completed"
        elif status_str == "failed":
            status_display = "✗ failed"
        elif status_str == "running":
            status_display = "⟳ running"
        elif status_str == "pending":
            status_display = "○ pending"
        elif status_str == "cancelled":
            status_display = "⊘ cancelled"
        else:
            status_display = status_str

        logger.info(f"  {name}")
        logger.info(f"    ID: {bt_id}")
        logger.info(f"    Project: {project_display}")
        logger.info(f"    Status: {status_display}")
        logger.info(f"    Created: {created}")

        if status_str == "completed" and bt_id:
            logger.info(f"    Pull results: lean cloud backtest --pull {bt_id}")

        if status_str == "failed" and bt.get("error"):
            error_preview = bt["error"][:100] + "..." if len(bt.get("error", "")) > 100 else bt.get("error", "")
            logger.info(f"    Error: {error_preview}")

        logger.info("")


def _run_cascade_backtest(
    project: str,
    name: str,
    push: bool,
    wait: bool,
    parameters: dict,
    start_date: Optional[str],
    end_date: Optional[str],
    initial_capital: float,
    data_provider_historical: Optional[str] = None,
) -> None:
    """Run a backtest using the Cascade data server.

    :param project: project name or path
    :param name: backtest name
    :param push: whether to push changes first
    :param wait: whether to wait for completion
    :param parameters: algorithm parameters
    :param start_date: optional start date
    :param end_date: optional end date
    :param initial_capital: initial capital
    """
    logger = container.logger
    data_server_client = container.data_server_client
    data_server_push_manager = container.data_server_push_manager

    # Push project if requested
    if push:
        project_path = Path.cwd() / project
        if project_path.exists() and project_path.is_dir():
            logger.info(f"Pushing project '{project}' to data server...")
            data_server_push_manager.push_project(project_path)
        else:
            raise RuntimeError(f"Project directory '{project}' not found")

    # Get project from data server
    try:
        data_server_project = data_server_client.get_project_by_name(project)
    except Exception as e:
        # Try to get by ID if name lookup fails
        try:
            data_server_project = data_server_client.get_project(project)
        except Exception:
            raise RuntimeError(
                f'No project with the given name or id "{project}" found in the data server. '
                f"Use --push to push your local project first."
            ) from e

    project_id = data_server_project.id
    logger.info(f"Found project '{data_server_project.name}' (id: {project_id})")

    # Normalize cascade provider aliases to their full names
    if data_provider_historical is not None:
        data_provider_historical = normalize_data_provider_historical(data_provider_historical)

    # Get provider config associations so the server knows which modules to use
    provider_config = get_cascade_provider_config(data_provider_historical) if data_provider_historical else None

    # Create backtest job
    logger.info(f"Creating backtest '{name}'...")
    backtest = data_server_client.create_backtest(
        project_id=project_id,
        name=name,
        parameters=parameters,
        start_date=start_date,
        end_date=end_date,
        initial_capital=initial_capital,
        data_provider_historical=data_provider_historical,
        provider_config=provider_config,
    )

    backtest_id = backtest["id"]
    logger.info(f"Backtest created: {backtest_id}")
    logger.info(f"Status: {backtest['status']}")

    if not wait:
        logger.info("Backtest queued for processing. Use --wait to wait for completion.")
        return

    # Poll for completion
    logger.info("Waiting for backtest to complete...")
    poll_count = 0
    while True:
        backtest = data_server_client.get_backtest(backtest_id)
        status = backtest["status"]

        if status in ("completed", "failed", "cancelled"):
            break

        poll_count += 1
        if poll_count % 6 == 0:  # Log every 30 seconds
            logger.info(f"Status: {status}...")

        time.sleep(5)

    # Display results
    logger.info(f"Backtest {status}")

    if status == "completed":
        # Download and save results locally
        results = None
        logs = None

        logger.info("Downloading results...")
        try:
            results = data_server_client.get_backtest_results(backtest_id)
        except Exception as e:
            logger.warn(f"Could not download results: {e}")

        logger.info("Downloading logs...")
        try:
            logs = data_server_client.get_backtest_logs(backtest_id)
        except Exception as e:
            logger.warn(f"Could not download logs: {e}")

        project_path = _resolve_project_path(data_server_project.name)
        output_dir = _save_backtest_results_locally(project_path, backtest, results, logs)
        _display_backtest_statistics(backtest, results)
        logger.info("")
        logger.info(f"Results saved to: {output_dir}")

    elif status == "failed":
        error = backtest.get("error", "Unknown error")
        logger.error("Backtest failed:")
        logger.error(error)

    elif status == "cancelled":
        logger.info("Backtest was cancelled")


@command(cls=LeanCommand)
@argument("project", type=str, required=False)
@option("--list", "list_backtests",
              is_flag=True,
              default=False,
              help="List backtests instead of running one")
@option("--status",
              type=str,
              help="Filter by status when listing (pending, running, completed, failed, cancelled)")
@option("--pull",
              type=str,
              default=None,
              metavar="BACKTEST_ID",
              help="Pull results of a completed backtest by ID or name and save them locally")
@option("--name", type=str, help="The name of the backtest (a random one is generated if not specified)")
@option("--push",
              is_flag=True,
              default=False,
              help="Push local modifications to the cloud before running the backtest")
@option("--open", "open_browser",
              is_flag=True,
              default=False,
              help="Automatically open the results in the browser when the backtest is finished")
@option("--wait",
              is_flag=True,
              default=False,
              help="Wait for the backtest to complete and save results locally")
@option("--start-date",
              type=str,
              help="Backtest start date (format: YYYY-MM-DD)")
@option("--end-date",
              type=str,
              help="Backtest end date (format: YYYY-MM-DD)")
@option("--initial-capital",
              type=float,
              default=100000,
              help="Initial capital for the backtest (default: 100000)")
@option("--data-provider-historical",
              type=str,
              default=None,
              help="Historical data provider (e.g., Local, thetadata, kalshi, hyper). Defaults to thetadata for cloud backtests.")
@backtest_parameter_option
def backtest(
    project: Optional[str],
    list_backtests: bool,
    status: Optional[str],
    pull: Optional[str],
    name: Optional[str],
    push: bool,
    open_browser: bool,
    wait: bool,
    start_date: Optional[str],
    end_date: Optional[str],
    initial_capital: float,
    data_provider_historical: Optional[str],
    parameter: List[Tuple[str, str]],
) -> None:
    """Backtest a project in the cloud.

    PROJECT must be the name or id of the project to run a backtest for.

    If the project that has to be backtested has been pulled to the local drive
    with `lean cloud pull` it is possible to use the --push option to push local
    modifications to the cloud before running the backtest.

    When using the Cascade data server (configured via `lean login`), backtests
    are queued for processing by the lean_worker service. Use --wait to wait
    for completion and automatically save results locally.

    Use --pull BACKTEST_ID to download results of an already-completed backtest.

    Use --list to see existing backtests (optionally filter by project and status).
    """
    logger = container.logger

    # Handle --list mode
    if list_backtests:
        if container.data_server_client is None:
            raise RuntimeError("--list is only supported with Cascade data server. Configure with `lean login`.")
        _list_cascade_backtests(project, status)
        return

    # Handle --pull mode: download results for an existing completed backtest
    if pull is not None:
        if container.data_server_client is None:
            raise RuntimeError("--pull is only supported with Cascade data server. Configure with `lean login`.")
        _pull_cascade_backtest_results(pull, project)
        return

    # For running a backtest, project is required
    if not project:
        raise RuntimeError("PROJECT argument is required. Use --list to see existing backtests.")

    if name is None:
        name = container.name_generator.generate_name()

    parameters = container.lean_config_manager.get_parameters(parameter)

    # Check if data server is configured
    if container.data_server_client is not None:
        # Use Cascade data server for backtest
        _run_cascade_backtest(
            project=project,
            name=name,
            push=push,
            wait=wait,
            parameters=parameters,
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            data_provider_historical=data_provider_historical,
        )
        return

    # Fall back to QuantConnect cloud
    cloud_project_manager = container.cloud_project_manager
    try:
        cloud_project = cloud_project_manager.get_cloud_project(project, push)
    except RuntimeError as e:
        if cloud_project_manager._project_config_manager.try_get_project_config(Path.cwd() / project):
            error_message = f'No project with the given name or id "{project}" found in your cloud projects.'
            error_message += f" Please use `lean cloud backtest --push {project}` to backtest in cloud."
        else:
            error_message = f'No project with the given name or id "{project}" found in your cloud or local projects.'
        raise RuntimeError(error_message)

    cloud_runner = container.cloud_runner
    finished_backtest = cloud_runner.run_backtest(cloud_project, name, parameters)

    if finished_backtest.error is None and finished_backtest.stacktrace is None:
        logger.info(finished_backtest.get_statistics_table())

    logger.info(f"Backtest id: {finished_backtest.backtestId}")
    logger.info(f"Backtest name: {finished_backtest.name}")
    logger.info(f"Backtest url: {finished_backtest.get_url()}")

    if finished_backtest.error is not None or finished_backtest.stacktrace is not None:
        error = finished_backtest.stacktrace or finished_backtest.error
        error = error.strip()

        logger.error("An error occurred during this backtest:")
        logger.error(error)

        # Don't open the results in the browser if the error happened during initialization
        # In the browser the logs won't show these errors, you'll just get empty charts and empty logs
        if error.startswith("During the algorithm initialization, the following exception has occurred:"):
            open_browser = False

    if open_browser:
        from webbrowser import open
        open(finished_backtest.get_url())
