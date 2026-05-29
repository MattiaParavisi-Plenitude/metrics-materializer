import os
from typing import Optional


class BaseConfig:
    FLASK_ENV: str = os.getenv("FLASK_ENV", "production")
    DEBUG: bool = False
    TESTING: bool = False

    SECRET_KEY: Optional[str] = os.getenv("SECRET_KEY", "dev-secret-key")
    DATABRICKS_HOST: Optional[str] = os.getenv("DATABRICKS_HOST", "")
    DATABRICKS_TOKEN: Optional[str] = os.getenv("DATABRICKS_TOKEN", "")
    DATABRICKS_WAREHOUSE_ID: Optional[str] = os.getenv("DATABRICKS_WAREHOUSE_ID", "be371ccfd95a94d1")

    APP_VERSION: str = os.getenv("APP_VERSION", "1.0")

    SESSION_COOKIE_HTTPONLY: bool = True
    SESSION_COOKIE_SAMESITE: str = "Lax"


class ProductionConfig(BaseConfig):
    ENV: str = "production"
    DEBUG: bool = False
    SESSION_COOKIE_SECURE: bool = True
    PREFERRED_URL_SCHEME: str = "https"
    TEMPLATES_AUTO_RELOAD: bool = False


class DevelopmentConfig(BaseConfig):
    ENV: str = "development"
    DEBUG: bool = True
    SESSION_COOKIE_SECURE: bool = False
    TEMPLATES_AUTO_RELOAD: bool = True


def get_config_class() -> type:
    env = (os.getenv("FLASK_ENV") or "production").strip().lower()
    if env.startswith("dev"):
        return DevelopmentConfig
    return ProductionConfig


Config = get_config_class()
