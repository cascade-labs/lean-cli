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
from typing import List, Dict, Optional
from time import sleep

from lean.components.api.data_server_client import DataServerClient
from lean.components.config.project_config_manager import ProjectConfigManager
from lean.components.util.logger import Logger
from lean.components.util.project_manager import ProjectManager
from lean.models.errors import RequestFailedError


class DataServerPushManager:
    """The DataServerPushManager class is responsible for synchronizing local projects to the data server."""

    def __init__(self,
                 logger: Logger,
                 data_server_client: DataServerClient,
                 project_manager: ProjectManager,
                 project_config_manager: ProjectConfigManager) -> None:
        """Creates a new DataServerPushManager instance.

        :param logger: the logger to use when printing messages
        :param data_server_client: the DataServerClient instance to use when communicating with the data server
        :param project_manager: the ProjectManager to use when looking for projects
        :param project_config_manager: the ProjectConfigManager instance to use
        """
        self._logger = logger
        self._data_server_client = data_server_client
        self._project_manager = project_manager
        self._project_config_manager = project_config_manager

    def push_project(self, project: Path) -> None:
        """Pushes the given project from the local drive to the data server.

        :param project: path to the directory containing the local project that needs to be pushed
        """
        relative_path = project.relative_to(Path.cwd())
        self._logger.info(f"[1/1] Pushing '{relative_path}'")
        self._push_project(project)

    def push_projects(self, projects_to_push: List[Path]) -> None:
        """Pushes the given projects from the local drive to the data server.

        :param projects_to_push: a list of directories containing the local projects that need to be pushed
        """
        if len(projects_to_push) == 0:
            return

        for index, path in enumerate(projects_to_push, start=1):
            relative_path = path.relative_to(Path.cwd())
            try:
                self._logger.info(f"[{index}/{len(projects_to_push)}] Pushing '{relative_path}'")
                self._push_project(path)
            except Exception as ex:
                from traceback import format_exc
                self._logger.debug(format_exc().strip())
                self._logger.warn(f"Cannot push '{relative_path}': {ex}")

    def _get_files(self, project: Path) -> List[Dict[str, str]]:
        """Gets the files of a local project for pushing.

        :param project: the local project to get files from
        :return: list of dicts with 'name' and 'content' keys
        """
        paths = self._project_manager.get_source_files(project)
        files = [
            {
                'name': path.relative_to(project).as_posix(),
                'content': path.read_text(encoding="utf-8")
            }
            for path in paths
        ]
        return files

    @staticmethod
    def _is_missing_project_error(error: RequestFailedError) -> bool:
        """Returns True when an API error indicates a missing project."""
        status_code = getattr(error.response, "status_code", None)
        if status_code == 404:
            return True

        response_text = str(getattr(error.response, "text", "") or "").lower()
        return ("no project with the given name or id" in response_text
                or "project not found" in response_text)

    @staticmethod
    def _response_text(error: RequestFailedError) -> str:
        return str(getattr(error.response, "text", "") or "").strip()

    @staticmethod
    def _is_transient_schema_cache_error(error: RequestFailedError) -> bool:
        response_text = DataServerPushManager._response_text(error).lower()
        return "pgrst002" in response_text or "schema cache" in response_text

    def _push_project(self, project_path: Path) -> None:
        """Pushes a single local project to the data server.

        Raises an error with a descriptive message if the project cannot be pushed.

        :param project_path: the local project to push
        """
        project_name = project_path.relative_to(Path.cwd()).as_posix()
        project_config = self._project_config_manager.get_project_config(project_path)
        data_server_id = project_config.get("data-server-id")

        files = self._get_files(project_path)
        description = project_config.get("description", "")
        algorithm_language = project_config.get("algorithm-language", "Python")
        parameters = project_config.get("parameters", {})

        if data_server_id is not None:
            # Project exists in data server, update it
            try:
                cloud_project = self._data_server_client.update_project(
                    data_server_id,
                    files=files,
                    description=description,
                    algorithm_language=algorithm_language,
                    parameters=parameters
                )
                self._logger.info(f"Successfully updated '{project_name}' in data server")
            except RequestFailedError as e:
                if self._is_missing_project_error(e):
                    # Project was deleted from server, create a new one
                    self._logger.info(f"Project '{project_name}' not found in data server, creating new...")
                    data_server_id = None
                else:
                    # Some servers can return generic 5xx errors for missing IDs.
                    # If the project ID can no longer be fetched, recover by creating a new project.
                    try:
                        self._data_server_client.get_project(data_server_id)
                    except RequestFailedError:
                        self._logger.info(f"Project '{project_name}' could not be retrieved from data server, creating new...")
                        data_server_id = None
                    else:
                        raise

        if data_server_id is None:
            # Create new project in data server
            create_error = None
            cloud_project = None
            for attempt in range(1, 4):
                try:
                    cloud_project = self._data_server_client.create_project(
                        name=project_name,
                        files=files,
                        description=description,
                        algorithm_language=algorithm_language,
                        parameters=parameters
                    )
                    break
                except RequestFailedError as error:
                    create_error = error
                    if not self._is_transient_schema_cache_error(error) or attempt == 3:
                        break

                    self._logger.warn(
                        f"Create project failed with a transient server error (attempt {attempt}/3). Retrying..."
                    )
                    sleep(1.5 * attempt)

            if cloud_project is None:
                # Some servers may return 5xx for duplicate names or partial creates.
                # If a project with this name exists, update it instead.
                existing_project = None
                try:
                    existing_project = self._data_server_client.get_project_by_name(project_name)
                except RequestFailedError:
                    try:
                        projects = self._data_server_client.list_projects()
                        existing_project = next((project for project in projects if project.name == project_name), None)
                    except RequestFailedError:
                        existing_project = None

                if existing_project is None:
                    response_text = self._response_text(create_error)
                    if response_text:
                        raise RuntimeError(f"{create_error}\nServer response: {response_text}")
                    raise create_error

                self._logger.info(f"Project '{project_name}' already exists in data server, updating existing project...")
                cloud_project = self._data_server_client.update_project(
                    existing_project.id,
                    files=files,
                    description=description,
                    algorithm_language=algorithm_language,
                    parameters=parameters
                )

            project_config.set("data-server-id", cloud_project.id)
            self._logger.info(f"Successfully created '{project_name}' in data server")
