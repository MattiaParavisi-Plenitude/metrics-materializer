import logging
import os
import sys

_base_dir = os.path.dirname(os.path.abspath(__file__))
_lib_path = os.path.join(_base_dir, "library")
if _lib_path not in sys.path:
    sys.path.insert(0, _lib_path)

from flask import Flask

from config import Config
from webapp.backend.core import get_core_blueprints
from webapp.backend.core.runtime import inject_template_context

log = logging.getLogger("werkzeug")
log.setLevel(logging.DEBUG)


def create_app() -> Flask:
    app = Flask(
        __name__,
        template_folder="webapp/frontend",
        static_folder="webapp/static",
    )

    app.config.from_object(Config)
    app.secret_key = app.config.get("SECRET_KEY", "dev-secret-key")
    app.config["TEMPLATES_AUTO_RELOAD"] = True

    for bp in get_core_blueprints():
        app.register_blueprint(bp)

    app.context_processor(inject_template_context)

    return app


if __name__ == "__main__":
    application = create_app()
    application.run(debug=True)
