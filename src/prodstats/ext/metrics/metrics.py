import logging
from typing import Dict, List, Tuple, Union

import config as conf

logger = logging.getLogger(__name__)


datadog = None
api_endpoint = "https://api.datadoghq.com/api/"

__all__ = ["load", "post", "post_event", "post_heartbeat", "to_tags"]


def load():
    """ Load and initialize the Datadog library """

    if conf.DATADOG_ENABLED:
        if conf.DATADOG_API_KEY is not None:
            global datadog
            import datadog

            datadog.initialize(
                api_key=str(conf.DATADOG_API_KEY),
                app_key=str(conf.DATADOG_APP_KEY),
                # api_host=api_endpoint,
            )
            logger.info("Datadog enabled")
        else:
            logger.warning("Failed to load Datadog configuration: missing api key")
    else:
        logger.debug("Datadog disabled.")


def post(
    name: str,
    points: Union[int, float, List[Tuple]],
    metric_type: str = "count",
    tags: list = None,
):
    """ Send a metric through the Datadog http api.

        Example:
                    api.Metric.post(
                        metric='my.series',
                        points=[
                            (now, 15),
                            (future_10s, 16)
                        ],
                        metric_type="count",
                        tags=["tag1", "tag2"]
                    )

    Arguments:
        name {str} -- metric name
        points {Union[int, float, List[Tuple]]} -- metric value(s)
    """
    try:
        # name = f"{project}.{name}".lower()
        name = name.lower()
        tags = (
            to_tags(conf.DATADOG_DEFAULT_TAGS)
            + to_tags(tags or [])
            + to_tags({"service_name": {conf.project}})
        )
        if datadog:
            result = datadog.api.Metric.send(
                metric=name, points=points, type=str(metric_type).lower(), tags=tags,
            )
            if result.get("status") == "ok":
                logger.debug(
                    "Datadog metric successfully sent: name=%s, points=%s",
                    name,
                    points,
                )
            else:
                logger.debug(
                    "Problem sending Datadog metric: status=%s, name=%s, points=%s",
                    result.get("status"),
                    name,
                    points,
                )
        else:
            logger.debug(
                "Datadog not configured. Suppressing metric name=%s, points=%s",
                name,
                points,
            )
    except Exception as e:
        logger.debug("Failed to send Datadog metric: %s", e)


def post_event(title: str, text: str, tags: Union[Dict, List, str] = None):
    """ Send an event through the Datadog http api. """
    try:
        if datadog:
            datadog.api.Event.create(title=title, text=text, tags=to_tags(tags or []))
    except Exception as e:
        logger.debug("Failed to send Datadog event: %s", e)


def post_heartbeat():
    """ Send service heartbeat to Datadog """
    logger.debug("heartbeat")
    return post("heartbeat", 1, metric_type="gauge")


def to_tags(values: Union[Dict, List, str], sep: str = ",") -> List[str]:
    """ Coerce the passed values into a list of colon separated key-value pairs.

        dict example:
            {"tag1": "value1", "tag2": "value2", ...}
                 -> ["tag1:value1", "tag2:value2", ...]

        list example:
            ["tag1", "tag2", ...] -> ["tag1", "tag2", ...]

        str example (comma-delimited):
            "tag1:value1, tag2:value2", ..." -> ["tag1:value1", "tag2:value2", ...]

        str example (single):
            "tag1:value1" -> ["tag1:value1"]
    """
    result: List[str] = []
    if isinstance(values, dict):
        result = [
            f"{key}:{str(value).lower().replace(' ','_')}"
            for key, value in values.items()
            if isinstance(value, (str, int))
        ]
    elif isinstance(values, str):
        if "," in values:
            result = values.split(sep)
        else:
            result = [values]
    elif isinstance(values, list):
        result = values
    else:
        result = []

    return result


# if __name__ == "__main__":

#     from datetime import datetime

#     conf.DATADOG_ENABLED = True
#     conf.DATADOG_API_KEY._value
#     conf.DATADOG_APP_KEY._value

#     import datadog

#     datadog.initialize(
#         api_key=str(conf.DATADOG_API_KEY),
#         app_key=str(conf.DATADOG_APP_KEY),
#         # api_host=api_endpoint,
#     )

#     now = datetime.now().isoformat()
#     datadog.api.Metric.send(metric="prodstats.test", points=[(now, 1)], type="count")
