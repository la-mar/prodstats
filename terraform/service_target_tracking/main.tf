

variable "cluster_name" {
  description = "Name of the ECS cluster"
}

variable "service_name" {
  description = "Name of the service"
}

variable "min_capacity" {
  description = "Minimum number of tasks"
}

variable "max_capacity" {
  description = "Maximum number of tasks"
}

variable "scale_in_threshold" {
  description = "App autoscaling target values"
  type        = number
}

variable "scale_out_threshold" {
  description = "App autoscaling target values"
  type        = number
}

variable "scale_in_cooldown" {
  description = "App autoscaling scale in cooldown (seconds)"
  type        = string
  default     = "300"
}

variable "scale_out_cooldown" {
  description = "App autoscaling scale out cooldown (seconds)"
  type        = string
  default     = "300"
}

variable "metric_name" {
  description = "App autoscaling custom metric name"
  type        = string
  default     = "CPUUtilization"
}

variable "queue1" {
  description = "Name of queue for Cloudwatch Metric"
  type        = string
}

variable "queue2" {
  description = "Name of queue for Cloudwatch Metric"
  type        = string
}

resource "aws_appautoscaling_target" "ecs_target" {
  min_capacity       = var.min_capacity
  max_capacity       = var.max_capacity
  resource_id        = "service/${var.cluster_name}/${var.service_name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

resource "aws_appautoscaling_policy" "sqs_policy_scale_out" {
  # count              = var.sqs_policy ? 1 : 0
  name = "${aws_appautoscaling_target.ecs_target.resource_id}/app-autoscale-out"
  # policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.ecs_target.resource_id
  scalable_dimension = aws_appautoscaling_target.ecs_target.scalable_dimension
  service_namespace  = aws_appautoscaling_target.ecs_target.service_namespace

  step_scaling_policy_configuration {
    adjustment_type         = "ChangeInCapacity"
    cooldown                = var.scale_out_cooldown
    metric_aggregation_type = "Average"

    step_adjustment {
      metric_interval_lower_bound = 0
      scaling_adjustment          = 1.0
    }
  }
  depends_on = [aws_appautoscaling_target.ecs_target]

}

resource "aws_appautoscaling_policy" "sqs_policy_scale_in" {
  # count              = var.sqs_policy ? 1 : 0
  name = "${aws_appautoscaling_target.ecs_target.resource_id}/app-autoscale-in"
  # policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.ecs_target.resource_id
  scalable_dimension = aws_appautoscaling_target.ecs_target.scalable_dimension
  service_namespace  = aws_appautoscaling_target.ecs_target.service_namespace

  step_scaling_policy_configuration {
    adjustment_type         = "ChangeInCapacity"
    cooldown                = var.scale_in_cooldown
    metric_aggregation_type = "Average"

    step_adjustment {
      metric_interval_upper_bound = 0
      scaling_adjustment          = -1
    }
  }
  depends_on = [aws_appautoscaling_target.ecs_target]

}

resource "aws_cloudwatch_metric_alarm" "sqs_usage_high" {
  alarm_name                = "${var.cluster_name}/${var.service_name}/sqs-usage-high"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "2"
  threshold                 = var.scale_out_threshold
  alarm_description         = "Report the aggregate total of messages across two SQS queues"
  insufficient_data_actions = []
  alarm_actions             = [aws_appautoscaling_policy.sqs_policy_scale_out.arn]


  metric_query {
    id          = "e1"
    expression  = "m1+m2"
    label       = "Average # messages (${var.queue1}, ${var.queue2})"
    return_data = "true"
  }

  metric_query {
    id = "m1"

    metric {
      metric_name = "ApproximateNumberOfMessagesVisible"
      namespace   = "AWS/SQS"
      period      = "60"
      stat        = "Average"
      unit        = "Count"

      dimensions = {
        QueueName = var.queue1
      }
    }
  }

  metric_query {
    id = "m2"

    metric {
      metric_name = "ApproximateNumberOfMessagesVisible"
      namespace   = "AWS/SQS"
      period      = "60"
      stat        = "Average"
      unit        = "Count"

      dimensions = {
        QueueName = var.queue2
      }
    }
  }
}

resource "aws_cloudwatch_metric_alarm" "sqs_usage_low" {
  alarm_name                = "${var.cluster_name}/${var.service_name}/sqs-usage-low"
  comparison_operator       = "LessThanOrEqualToThreshold"
  evaluation_periods        = "2"
  threshold                 = var.scale_in_threshold
  alarm_description         = "Report the aggregate total of messages across two SQS queues"
  insufficient_data_actions = []
  alarm_actions             = [aws_appautoscaling_policy.sqs_policy_scale_in.arn]


  metric_query {
    id          = "e1"
    expression  = "m1+m2"
    label       = "Average # messages (${var.queue1}, ${var.queue2})"
    return_data = "true"
  }

  metric_query {
    id = "m1"

    metric {
      metric_name = "ApproximateNumberOfMessagesVisible"
      namespace   = "AWS/SQS"
      period      = "60"
      stat        = "Average"
      unit        = "Count"

      dimensions = {
        QueueName = var.queue1
      }
    }
  }

  metric_query {
    id = "m2"

    metric {
      metric_name = "ApproximateNumberOfMessagesVisible"
      namespace   = "AWS/SQS"
      period      = "60"
      stat        = "Average"
      unit        = "Count"

      dimensions = {
        QueueName = var.queue2
      }
    }
  }
