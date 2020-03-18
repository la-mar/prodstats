# """ Custom routing functions for Celery """
# import config as conf


# def router_mapper(name, args, kwargs, options, task=None, **kw):
#     """ Route the message to a queue specified in kwargs,
#         defaults to the configured default queue.
#     """

#     queue_name = conf.CELERY_DEFAULT_QUEUE

#     if kwargs:
#         mapped_queue = kwargs.get("queue", None)
#         if mapped_queue:
#             queue_name = mapped_queue

#     return {"queue": queue_name}
