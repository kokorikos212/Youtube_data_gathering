import sqlite3 as sql 
from pytube import Playlist, YouTube
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from itertools import cycle
import re
import os 
import time 
import sys 

import re
from urllib.parse import urlparse, parse_qs
from contextlib import suppress

from requests.auth import HTTPProxyAuth
import requests
import logging 

import traceback
import linecache