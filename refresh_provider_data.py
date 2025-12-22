#!/usr/bin/env python3
"""
refresh_provider_data.py
Manual helper to refresh provider_map.json and provider_metadata.json.
"""

import argparse
import logging
import os
import sys

from api_utils import DispatcharrAPI
from provider_data import refresh_provider_data
from stream_analysis import Config


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Refresh Dispatcharr provider metadata for Dispatcharr-Maid."
    )
    parser.add_argument(
        '--config',
        default='config.yaml',
        help='Path to config.yaml (default: config.yaml)'
    )
    parser.add_argument(
        '--working-dir',
        default=None,
        help='Working directory for config and output files'
    )
    parser.add_argument(
        '--manage-py',
        default=None,
        help='Path to Dispatcharr manage.py (overrides config/env)'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Overwrite existing provider_map.json and provider_metadata.json'
    )
    return parser


def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    parser = _build_parser()
    args = parser.parse_args()

    if args.manage_py:
        os.environ['DISPATCHARR_MANAGE_PY'] = args.manage_py

    config = Config(args.config, working_dir=args.working_dir)

    api = None
    if not (config.get('dispatcharr') or {}).get('manage_py_path') and not os.getenv('DISPATCHARR_MANAGE_PY'):
        try:
            api = DispatcharrAPI()
            api.login()
        except Exception as exc:
            logging.error("Dispatcharr API setup failed: %s", exc)
            return 1

    provider_map, provider_metadata = refresh_provider_data(api, config, force=args.force)
    if not provider_map and not provider_metadata:
        logging.error("Provider data refresh failed or no data available.")
        return 1

    logging.info(
        "Provider data refreshed: %s providers, metadata for %s providers.",
        len(provider_map),
        len(provider_metadata)
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
