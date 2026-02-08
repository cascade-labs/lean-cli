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

from typing import Dict, Optional


# Simplified aliases for Cascade data providers
# Note: "hyper" maps to "Hyperliquid" which is handled by cascade-modules.json
CASCADE_PROVIDER_ALIASES = {
    "thetadata": "CascadeThetaData",
    "kalshi": "CascadeKalshiData",
    "hyper": "Hyperliquid",
    "polygon": "Polygon",
}

# Full list of cascade providers (both aliases and full names for backward compatibility)
# Hyperliquid is already in cli_data_downloaders via cascade-modules.json, so "hyper" is just an alias
CASCADE_PROVIDERS = ["thetadata", "kalshi", "hyper", "CascadeThetaData", "CascadeKalshiData"]


def normalize_data_provider_historical(name: str) -> str:
    """Normalize aliases like 'thetadata' -> 'CascadeThetaData'.

    :param name: the provider name or alias
    :return: the normalized provider name
    """
    return CASCADE_PROVIDER_ALIASES.get(name.lower(), name)


def get_cascade_provider_config(provider_name: str) -> Optional[Dict[str, str]]:
    """Return the lean config associations for a Cascade provider, or None if not a Cascade provider.

    This is the single source of truth for all commands (backtest, live, research, optimize, cloud backtest).

    :param provider_name: the normalized provider name (e.g. "CascadeThetaData", not "thetadata")
    :return: dict with lean config keys, or None for non-Cascade providers
    """
    if provider_name == "CascadeThetaData":
        return {
            "data-provider": "QuantConnect.Lean.Engine.DataFeeds.DownloaderDataProvider",
            "data-downloader": "QuantConnect.Lean.DataSource.CascadeThetaData.CascadeThetaDataDownloader",
            "history-provider": "QuantConnect.Lean.DataSource.CascadeThetaData.CascadeThetaDataProvider",
            "map-file-provider": "QuantConnect.Lean.DataSource.CascadeThetaData.ThetaDataMapFileProvider",
            "factor-file-provider": "QuantConnect.Lean.DataSource.CascadeThetaData.ThetaDataFactorFileProvider",
        }
    elif provider_name == "CascadeKalshiData":
        return {
            "data-provider": "QuantConnect.Lean.Engine.DataFeeds.DownloaderDataProvider",
            "data-downloader": "QuantConnect.Lean.DataSource.CascadeKalshiData.CascadeKalshiDataDownloader",
            "history-provider": "QuantConnect.Lean.DataSource.CascadeKalshiData.CascadeKalshiDataProvider",
            "map-file-provider": "QuantConnect.Data.Auxiliary.LocalDiskMapFileProvider",
        }
    elif provider_name == "Hyperliquid":
        return {
            "data-provider": "QuantConnect.Lean.Engine.DataFeeds.DownloaderDataProvider",
            "data-downloader": "QuantConnect.Lean.DataSource.CascadeHyperliquid.HyperliquidDataDownloader",
            "history-provider": "QuantConnect.Lean.DataSource.CascadeHyperliquid.HyperliquidHistoryProvider",
        }
    elif provider_name == "Polygon":
        return {
            "data-provider": "QuantConnect.Lean.Engine.DataFeeds.DownloaderDataProvider",
            "data-downloader": "QuantConnect.Lean.DataSource.Polygon.PolygonDataDownloader",
            "history-provider": "QuantConnect.Lean.DataSource.Polygon.PolygonDataProvider",
            "map-file-provider": "QuantConnect.Lean.DataSource.Polygon.PolygonMapFileProvider",
            "factor-file-provider": "QuantConnect.Lean.DataSource.Polygon.PolygonFactorFileProvider",
        }
    return None
