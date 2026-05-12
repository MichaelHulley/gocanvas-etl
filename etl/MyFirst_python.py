# =========================================
# GoCanvas Intake ETL Script
# Author: Michael Hulley
# Date: 2026-04-13
# Description:
#   First Python code 
# =========================================

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pyodbc
import requests
from dotenv import load_dotenv
from gocanvas_get_token import get_access_token
from tqdm import tqdm

load_dotenv()

# =========================
# CONFIG
# =========================

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DB = os.getenv("SQL_DB") or os.getenv("SQL_DATABASE")
SQL_USER = os.getenv("SQL_USER") or os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")