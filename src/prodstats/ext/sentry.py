import logging

import config as conf

logger = logging.getLogger(__name__)


def load():
    # logger.warning(f"Attempting to load Sentry")
    if conf.SENTRY_ENABLED:
        import sentry_sdk
        from sentry_sdk.integrations.logging import LoggingIntegration
        from sentry_sdk.integrations.redis import RedisIntegration

        # from sentry_sdk.integrations.asgi import SentryAsgiMiddleware
        try:
            if conf.SENTRY_DSN is not None and conf.SENTRY_DSN != "":
                dsn = conf.SENTRY_DSN
                event_level = conf.SENTRY_EVENT_LEVEL
                release = conf.SENTRY_RELEASE
                env_name = conf.SENTRY_ENV_NAME
                breadcrumb_level = conf.SENTRY_LEVEL

                sentry_logging = LoggingIntegration(
                    level=breadcrumb_level,  # Capture level and above as breadcrumbs
                    event_level=event_level,  # Send errors as events
                )

                sentry_integrations = [
                    sentry_logging,
                    # CeleryIntegration(),
                    RedisIntegration(),
                ]

                s = ", ".join([x.identifier for x in sentry_integrations])
                sentry_sdk.init(
                    dsn=dsn,
                    release=release,
                    integrations=sentry_integrations,
                    environment=env_name,
                )
                logger.info(
                    f"Sentry enabled with {len(sentry_integrations)} integrations: {s}"
                )

            else:
                logger.warning(f"Sentry DSN is missing or empty")

        except Exception as e:
            logger.error(f"Failed to load Sentry configuration: {e}")

    else:
        logger.debug(f"Sentry disabled")
