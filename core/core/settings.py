import os
from datetime import timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

def _csv(name, default=""):
    # Unset or empty -> default (an empty DJANGO_ALLOWED_HOSTS in .env must not mean "no hosts").
    return [item.strip() for item in (os.environ.get(name) or default).split(",") if item.strip()]


SECRET_KEY = os.environ.get("DJANGO_SECRET_KEY", "dev-insecure-secret")
DEBUG = os.environ.get("DJANGO_DEBUG", "0") == "1"
ALLOWED_HOSTS = _csv("DJANGO_ALLOWED_HOSTS", "*")
CSRF_TRUSTED_ORIGINS = ["http://localhost", "http://localhost:8000", *_csv("CSRF_TRUSTED_ORIGINS")]

# Deployed behind Caddy or Cloudflare (HTTPS) -> nginx (HTTP): trust their X-Forwarded-Proto.
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
SESSION_COOKIE_SECURE = not DEBUG
CSRF_COOKIE_SECURE = not DEBUG

# The React app on Vercel calls this API from another origin (docs/DEPLOY.md). Empty = same origin only.
CORS_ALLOWED_ORIGINS = _csv("CORS_ALLOWED_ORIGINS")

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "rest_framework",
    "corsheaders",
    "sales",
]

MIDDLEWARE = [
    "corsheaders.middleware.CorsMiddleware",  # before CommonMiddleware, answers preflight requests
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",  # serves admin CSS/JS from STATIC_ROOT under gunicorn
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "core.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "core.wsgi.application"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.environ.get("POSTGRES_DB", "flashcore"),
        "USER": os.environ.get("POSTGRES_USER", "flashcore"),
        "PASSWORD": os.environ.get("POSTGRES_PASSWORD", "flashcore"),
        "HOST": os.environ.get("POSTGRES_HOST", "postgres"),
        "PORT": os.environ.get("POSTGRES_PORT", "5432"),
        "CONN_MAX_AGE": 60,
    }
}

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
]

REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": (
        "rest_framework_simplejwt.authentication.JWTAuthentication",
    ),
    "DEFAULT_PERMISSION_CLASSES": ("rest_framework.permissions.AllowAny",),
}

# Django signs, the FastAPI gateway verifies with the same key (docs/ARCHITECTURE.md section 4).
SIMPLE_JWT = {
    "ALGORITHM": "HS256",
    "SIGNING_KEY": os.environ.get("JWT_SIGNING_KEY", "dev-only-change-me-0123456789abcdef"),
    "USER_ID_CLAIM": "user_id",
    "ACCESS_TOKEN_LIFETIME": timedelta(hours=1),
    "REFRESH_TOKEN_LIFETIME": timedelta(days=1),
}

CELERY_BROKER_URL = os.environ.get("CELERY_BROKER_URL", "redis://redis:6379/1")
CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP = True
CELERY_TASK_ACKS_LATE = True                 # ack after the task finishes, not when it starts
CELERY_TASK_REJECT_ON_WORKER_LOST = True     # a killed worker's task goes back to the queue
CELERY_WORKER_PREFETCH_MULTIPLIER = 1
CELERY_TASK_IGNORE_RESULT = True
CELERY_BROKER_TRANSPORT_OPTIONS = {"visibility_timeout": 120}  # unacked tasks are redelivered after 2 min
CELERY_WORKER_HIJACK_ROOT_LOGGER = False     # keep our JSON log format
# Celery's Django fixup closes the DB connection after EVERY task unless this is set. Measured:
# ~160 new PostgreSQL connections/s (one forked backend per task), postgres at ~4 CPU cores.
# With reuse, each worker process keeps its connection and recycles it every 2,000 tasks.
CELERY_DB_REUSE_MAX = 1000
CELERY_BEAT_SCHEDULE = {
    "reconcile": {"task": "sales.tasks.reconcile", "schedule": 60.0},
}

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_ORDER_TOPIC = os.environ.get("KAFKA_ORDER_TOPIC", "order-events")

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"json": {"()": "sales.log_format.JsonFormatter"}},
    "handlers": {"console": {"class": "logging.StreamHandler", "formatter": "json"}},
    "root": {"handlers": ["console"], "level": "INFO"},
}
