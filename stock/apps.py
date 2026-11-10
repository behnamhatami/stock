from django.apps import AppConfig


class DjangoCronConfig(AppConfig):
    """django_cron keeps its own id type, so DEFAULT_AUTO_FIELD = BigAutoField
    doesn't make `makemigrations` write files into site-packages."""
    name = "django_cron"
    default_auto_field = "django.db.models.AutoField"