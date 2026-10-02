"""Canonical configuration for the lake.

Every value here is safe to commit: there are no secrets in v1. The only
credential-adjacent knob is the SEC User-Agent, which is an identification
string rather than a secret — SEC requires it so they can contact a
responsible party, and it is deliberately not treated as confidential.
"""

from __future__ import annotations

import os
from pathlib import Path

# SEC requires every request to identify a contactable party and throttles
# requests that do not. This is an email, not a credential.
SEC_USER_AGENT = "DataEngineeringPortfolio/0.1 (sujalbirwadkar19@gmail.com)"

# Default sink for development and tests. No cloud dependency, no cost.
LAKE_ROOT = Path(os.environ.get("DE_LAKE_ROOT", "./lake")).resolve()

# Set DE_LAKE_S3_BUCKET to switch to the S3 backend. Works unchanged against
# Cloudflare R2, MinIO, Garage, or AWS S3 — they all speak the S3 API.
S3_BUCKET = os.environ.get("DE_LAKE_S3_BUCKET")
S3_ENDPOINT_URL = os.environ.get("DE_LAKE_S3_ENDPOINT")
S3_ACCESS_KEY = os.environ.get("DE_LAKE_S3_ACCESS_KEY")
S3_SECRET_KEY = os.environ.get("DE_LAKE_S3_SECRET_KEY")
S3_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
