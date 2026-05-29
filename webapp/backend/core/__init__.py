from __future__ import annotations

import importlib
from typing import List

from flask import Blueprint

CORE_MODULE = "webapp.backend.core.routes"


def get_core_blueprints() -> List[Blueprint]:
    module = importlib.import_module(CORE_MODULE)
    blueprints = []
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if isinstance(attr, Blueprint):
            blueprints.append(attr)
    return blueprints
