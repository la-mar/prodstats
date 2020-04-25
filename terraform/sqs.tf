locals {
  hz = "horizontal"
  vt = "vertical"
}


data "aws_iam_policy_document" "allow_ecs_task_access_to_sqs" {
  statement {
    sid = "0" # task_access_sqs
    actions = [
      "sqs:*",
    ]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values = [
        aws_ecs_service.worker.id,
        aws_ecs_service.cron.id,
      ]
    }
    resources = [
      aws_sqs_queue.default.arn,
      aws_sqs_queue.h.arn,
      aws_sqs_queue.v.arn,
    ]
  }
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

resource "aws_sqs_queue_policy" "default" {
  queue_url = aws_sqs_queue.default.id
  policy    = data.aws_iam_policy_document.allow_ecs_task_access_to_sqs.json
}

resource "aws_sqs_queue_policy" "h" {
  queue_url = aws_sqs_queue.h.id
  policy    = data.aws_iam_policy_document.allow_ecs_task_access_to_sqs.json
}

resource "aws_sqs_queue_policy" "v" {
  queue_url = aws_sqs_queue.v.id
  policy    = data.aws_iam_policy_document.allow_ecs_task_access_to_sqs.json
}
