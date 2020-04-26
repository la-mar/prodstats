locals {
  hz = "horizontal"
  vt = "vertical"
}




# NOTE: visibility timeout must match or exceed longest anticipated eta/cooldown/retry
#       used in a given celery task.
#       see caveats: https://docs.celeryproject.org/en/latest/getting-started/brokers/sqs.html
resource "aws_sqs_queue" "default" {
  name                       = "${var.service_name}-default"
  delay_seconds              = 0        # hide message for x number of seconds before making available to consumers
  message_retention_seconds  = 3600 * 4 # 4 hours
  visibility_timeout_seconds = 3600 * 2 # 2 hours
  receive_wait_time_seconds  = 0
  tags                       = local.tags
}


resource "aws_sqs_queue" "h" {
  name                       = "${var.service_name}-h"
  delay_seconds              = 0         # hide message for x number of seconds before making available to consumers
  message_retention_seconds  = 3600 * 24 # 24 hours
  visibility_timeout_seconds = 3600      # 1 hour
  receive_wait_time_seconds  = 0
  tags                       = merge(local.tags, { class = local.hz })
}

resource "aws_sqs_queue" "v" {
  name                       = "${var.service_name}-v"
  delay_seconds              = 0        # hide message for x number of seconds before making available to consumers
  message_retention_seconds  = 3600 * 6 # 6 hours
  visibility_timeout_seconds = 3600 * 2 # 2 hours
  receive_wait_time_seconds  = 0
  tags                       = merge(local.tags, { class = local.vt })
}
