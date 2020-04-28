# General
variable "domain" {
  description = "Design domain of this service."
}

variable "environment" {
  description = "Environment Name"
}

variable "service_name" {
  description = "Name of the service"
}

variable "service_port" {
  description = "Web service port"
}

variable "worker_scale_in_threshold" {
  description = "Threshold to trigger a scale-in event, represented as number of messages in SQS"
  type        = number
}

variable "worker_scale_in_cooldown" {
  description = "Cooldown, in seconds, between triggering scale in events"
  type        = number
}

variable "worker_scale_out_threshold" {
  description = "Threshold to trigger a scale-out event, represented as number of messages in SQS"
  type        = number
}

variable "worker_scale_out_cooldown" {
  description = "Cooldown, in seconds, between triggering scale out events"
  type        = number
}

variable "worker_min_capacity" {
  description = "worker service minimum number of autoscaled containers"
  type        = number
  default     = 1
}

variable "worker_max_capacity" {
  description = "worker service minimum number of autoscaled containers"
  type        = number
}

variable "web_desired_count" {
  description = "desired number of web containers"
  type        = number
  default     = 2
}

variable "worker_desired_count" {
  description = "desired number of worker containers"
  type        = number
  default     = 2
}

variable "cron_desired_count" {
  description = "desired number of cron containers"
  type        = number
  default     = 2
}
