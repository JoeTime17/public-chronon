import os
import sys
from typing import Optional

CONFIG_ORIGIN_FILE_ATTR = "_chronon_config_origin_file"


def mark_factory_created_config(obj):
    origin_file = _factory_caller_file()
    if origin_file is not None:
        setattr(obj, CONFIG_ORIGIN_FILE_ATTR, origin_file)
    return obj


def get_factory_origin_file(obj) -> Optional[str]:
    origin_file = getattr(obj, CONFIG_ORIGIN_FILE_ATTR, None)
    return os.path.abspath(origin_file) if origin_file else None


def _factory_caller_file() -> Optional[str]:
    frame = sys._getframe()
    while frame is not None:
        module_name = frame.f_globals.get("__name__", "")
        if not module_name.startswith("ai.chronon."):
            return os.path.abspath(frame.f_code.co_filename)
        frame = frame.f_back
    return None
