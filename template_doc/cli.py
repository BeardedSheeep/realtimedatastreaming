import logging
import uuid

from template_doc.observability import configure_observability, set_correlation_context
from template_doc.settings import get_settings

logger = logging.getLogger(__name__)


def main() -> None:
    settings = get_settings()
    configure_observability(settings)
    set_correlation_context(request_id_value=str(uuid.uuid4()))

    logger.info("application_started")
    try:
        _print_user_output(f"{settings.app_name} [{settings.environment}]")
    except Exception:
        logger.exception("application_failed")
        raise
    logger.info("application_finished")


def _print_user_output(message: str) -> None:
    print(message)
