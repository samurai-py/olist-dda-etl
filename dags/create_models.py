import os
import logging
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag

from cosmos.profiles import DatabricksTokenProfileMapping