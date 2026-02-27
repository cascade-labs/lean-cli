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

from typing import Any, Dict, Optional

from lean.components.config.storage import Storage
from lean.constants import DEFAULT_ENGINE_IMAGE, DEFAULT_RESEARCH_IMAGE
from lean.models.docker import DockerImage
from lean.models.errors import MoreInfoError
from lean.models.options import ChoiceOption, Option


class CLIConfigManager:
    """The CLIConfigManager class contains all configurable CLI options."""

    def __init__(self, general_storage: Storage, credentials_storage: Storage) -> None:
        """Creates a new CLIConfigManager instance.

        :param general_storage: the Storage instance for general, non-sensitive options
        :param credentials_storage: the Storage instance for credentials
        """
        self._general_storage = general_storage
        self._credentials_storage = credentials_storage

        self.user_id = Option("user-id",
                              "The user id used when making authenticated requests to the QuantConnect API.",
                              True,
                              credentials_storage)

        self.api_token = Option("api-token",
                                "The API token used when making authenticated requests to the QuantConnect API.",
                                True,
                                credentials_storage)

        self.default_language = ChoiceOption("default-language",
                                             "The default language used when creating new projects.",
                                             ["python", "csharp"],
                                             False,
                                             general_storage,
                                             "python")

        self.engine_image = Option("engine-image",
                                   f"The Docker image used when running the LEAN engine ({DEFAULT_ENGINE_IMAGE} if not set).",
                                   False,
                                   general_storage)

        self.research_image = Option("research-image",
                                     f"The Docker image used when running the research environment ({DEFAULT_RESEARCH_IMAGE} if not set).",
                                     False,
                                     general_storage)
        self.database_update_frequency = Option("database-update-frequency",
                                                "How often the databases are updated. "
                                                "The format is DD.HH:MM:SS. If the frequency "
                                                "is less than a day can just be HH:MM:SS. "
                                                "Update can be disabled by setting this option to a non-date"
                                                " value (-, _, ..., etc.). "
                                                "If unset, default value is 1 day",
                                                False,
                                                general_storage)

        self.data_server_url = Option("data-server-url",
                                      "The URL of the data server.",
                                      False,
                                      credentials_storage)

        self.data_server_api_key = Option("data-server-api-key",
                                          "The API key for the data server.",
                                          True,
                                          credentials_storage)

        self.default_start_date = Option("default-start-date",
                                         "The default start date for backtests (format: YYYY-MM-DD).",
                                         False,
                                         general_storage)

        self.default_end_date = Option("default-end-date",
                                       "The default end date for backtests (format: YYYY-MM-DD).",
                                       False,
                                       general_storage)

        self.thetadata_url = Option("thetadata-url",
                                    "The ThetaData REST API URL.",
                                    False,
                                    credentials_storage)

        self.thetadata_api_key = Option("thetadata-api-key",
                                        "The API key for ThetaData (Bearer token).",
                                        True,
                                        credentials_storage)

        self.ghcr_token = Option("ghcr-token",
                                 "The GitHub Container Registry token for pulling private LEAN images.",
                                 True,
                                 credentials_storage)

        self.kalshi_api_key = Option("kalshi-api-key",
                                     "The API key (key ID) for Kalshi.",
                                     True,
                                     credentials_storage)

        self.kalshi_private_key_path = Option("kalshi-private-key-path",
                                              "Path to the Kalshi private key file (PEM format).",
                                              False,
                                              general_storage)

        self.kalshi_private_key = Option("kalshi-private-key",
                                         "The base64-encoded Kalshi private key (auto-set from path).",
                                         True,
                                         credentials_storage)

        self.s3_access_key = Option("s3-access-key",
                                     "The S3 access key.",
                                     True,
                                     credentials_storage)

        self.s3_secret_key = Option("s3-secret-key",
                                     "The S3 secret key.",
                                     True,
                                     credentials_storage)

        self.s3_endpoint = Option("s3-endpoint",
                                   "The S3-compatible endpoint.",
                                   False,
                                   general_storage)

        self.tradealert_s3_bucket = Option("tradealert-s3-bucket",
                                           "The TradeAlert S3 bucket name.",
                                           False,
                                           general_storage)

        self.s3_region = Option("s3-region",
                                 "The S3 region.",
                                 False,
                                 general_storage)

        self.lean_s3_bucket = Option("lean-s3-bucket",
                                     "The bucket name for lean container/CLI storage.",
                                     False,
                                     general_storage)

        self.hyperliquid_s3_bucket = Option("hyperliquid-s3-bucket",
                                            "The Hyperliquid S3 bucket name.",
                                            False,
                                            general_storage)

        self.container_registry = Option("container-registry",
                                         "The container registry endpoint (e.g., iad.ocir.io).",
                                         False,
                                         general_storage)

        self.container_registry_namespace = Option("container-registry-namespace",
                                                   "The container registry namespace.",
                                                   False,
                                                   general_storage)

        self.container_registry_username = Option("container-registry-username",
                                                  "The container registry username (e.g., namespace/email).",
                                                  False,
                                                  general_storage)

        self.container_registry_token = Option("container-registry-token",
                                               "The container registry auth token.",
                                               True,
                                               credentials_storage)

        self.polygon_api_key = Option("polygon-api-key",
                                      "The API key for Polygon.io.",
                                      True,
                                      credentials_storage)

        self.polygon_s3_endpoint = Option("polygon-s3-endpoint",
                                          "The S3 endpoint for Polygon flat files (e.g., files.massive.com).",
                                          False,
                                          general_storage)

        self.polygon_s3_access_key = Option("polygon-s3-access-key",
                                            "The S3 access key for Polygon flat files.",
                                            True,
                                            credentials_storage)

        self.polygon_s3_secret_key = Option("polygon-s3-secret-key",
                                            "The S3 secret key for Polygon flat files.",
                                            True,
                                            credentials_storage)

        self.polygon_s3_bucket = Option("polygon-s3-bucket",
                                        "The S3 bucket name for Polygon flat files.",
                                        False,
                                        general_storage)

        self.hyperliquid_aws_access_key_id = Option("hyperliquid-aws-access-key-id",
                                                     "The AWS access key ID for Hyperliquid S3 historical data.",
                                                     True,
                                                     credentials_storage)

        self.hyperliquid_aws_secret_access_key = Option("hyperliquid-aws-secret-access-key",
                                                         "The AWS secret access key for Hyperliquid S3 historical data.",
                                                         True,
                                                         credentials_storage)

        self.security_data_feeds = Option("security-data-feeds",
                                          "The security data feeds configuration (JSON string, e.g. '{ \"Equity\": [\"Trade\"] }').",
                                          False,
                                          general_storage)

        self.all_options = [
            self.user_id,
            self.api_token,
            self.default_language,
            self.engine_image,
            self.research_image,
            self.database_update_frequency,
            self.data_server_url,
            self.data_server_api_key,
            self.default_start_date,
            self.default_end_date,
            self.thetadata_url,
            self.thetadata_api_key,
            self.ghcr_token,
            self.kalshi_api_key,
            self.kalshi_private_key_path,
            self.kalshi_private_key,
            self.s3_access_key,
            self.s3_secret_key,
            self.s3_endpoint,
            self.tradealert_s3_bucket,
            self.s3_region,
            self.lean_s3_bucket,
            self.hyperliquid_s3_bucket,
            self.container_registry,
            self.container_registry_namespace,
            self.container_registry_username,
            self.container_registry_token,
            self.polygon_api_key,
            self.polygon_s3_endpoint,
            self.polygon_s3_access_key,
            self.polygon_s3_secret_key,
            self.polygon_s3_bucket,
            self.hyperliquid_aws_access_key_id,
            self.hyperliquid_aws_secret_access_key,
            self.security_data_feeds
        ]

    def _get_data_server_profiles(self) -> Dict[str, Dict[str, str]]:
        """Returns configured data server profiles, bootstrapping default if needed."""
        profiles = self._credentials_storage.get("data-server-profiles", {})
        if not isinstance(profiles, dict):
            profiles = {}

        if "default" not in profiles:
            profiles["default"] = {
                "data-server-url": self.data_server_url.get_value() or "",
                "data-server-api-key": self.data_server_api_key.get_value() or "",
                "config-name": "default"
            }
            self._credentials_storage.set("data-server-profiles", profiles)

        return profiles

    def list_data_server_profiles(self) -> Dict[str, Dict[str, str]]:
        """Returns all configured data server profiles."""
        return self._get_data_server_profiles()

    def get_active_data_server_profile_name(self) -> str:
        """Returns the active data server profile name."""
        active = self._general_storage.get("data-server-profile", "default")
        profiles = self._get_data_server_profiles()
        if active not in profiles:
            active = "default"
            self._general_storage.set("data-server-profile", active)
        return active

    def get_data_server_profile(self, name: str) -> Dict[str, str]:
        """Returns a named data server profile."""
        profiles = self._get_data_server_profiles()
        if name not in profiles:
            raise ValueError(f"Profile '{name}' does not exist")
        return profiles[name]

    def get_active_data_server_profile(self) -> Dict[str, str]:
        """Returns the currently active data server profile."""
        return self.get_data_server_profile(self.get_active_data_server_profile_name())

    def upsert_data_server_profile(self,
                                   name: str,
                                   data_server_url: Optional[str] = None,
                                   data_server_api_key: Optional[str] = None,
                                   config_name: Optional[str] = None) -> Dict[str, str]:
        """Creates or updates a data server profile."""
        if name == "":
            raise ValueError("Profile name cannot be empty")

        profiles = self._get_data_server_profiles()
        current = profiles.get(name, {})

        if data_server_url is not None:
            current["data-server-url"] = data_server_url
        if data_server_api_key is not None:
            current["data-server-api-key"] = data_server_api_key
        if config_name is not None:
            current["config-name"] = config_name

        if "config-name" not in current:
            current["config-name"] = "default" if name == "default" else name

        profiles[name] = current
        self._credentials_storage.set("data-server-profiles", profiles)
        return current

    def set_active_data_server_profile(self, name: str) -> Dict[str, str]:
        """Sets the active data server profile and applies its URL/API key options."""
        profile = self.get_data_server_profile(name)
        self._general_storage.set("data-server-profile", name)

        profile_url = profile.get("data-server-url", "")
        profile_api_key = profile.get("data-server-api-key", "")
        if profile_url:
            self.data_server_url.set_value(profile_url)
        if profile_api_key:
            self.data_server_api_key.set_value(profile_api_key)

        return profile

    def get_active_cloud_config_name(self) -> str:
        """Returns the cloud config name associated with the active profile."""
        profile = self.get_active_data_server_profile()
        return profile.get("config-name", "default")

    def get_option_by_key(self, key: str) -> Option:
        """Returns the option matching the given key.

        If no option with the given key exists, an error is raised.

        :param key: the key to look for
        :return: the option having a key equal to the given key
        """
        option = next((x for x in self.all_options if x.key == key), None)

        if option is None:
            raise MoreInfoError(f"There doesn't exist an option with key '{key}'",
                                "https://www.lean.io/docs/v2/lean-cli/api-reference/lean-config-set#02-Description")

        return option

    def get_engine_image(self, override: Optional[str] = None) -> DockerImage:
        """Returns the LEAN engine image to use.

        :param override: the image name to use, overriding any defaults or previously configured options
        :return: the image that should be used when running the LEAN engine
        """
        return self._get_image_name(self.engine_image, DEFAULT_ENGINE_IMAGE, override)

    def get_research_image(self, override: Optional[str] = None) -> DockerImage:
        """Returns the LEAN research image to use.

        :param override: the image name to use, overriding any defaults or previously configured options
        :return: the image that should be used when running the research environment
        """
        return self._get_image_name(self.research_image, DEFAULT_RESEARCH_IMAGE, override)

    def _get_image_name(self, option: Option, default: str, override: Optional[str]) -> DockerImage:
        """Returns the image to use.

        :param option: the CLI option that configures the image type
        :param override: the image name to use, overriding any defaults or previously configured options
        :param default: the default image to use when the option is not set and no override is given
        :return: the image to use
        """
        if override is not None:
            image = override
        else:
            image = option.get_value(default)

        return DockerImage.parse(image)
