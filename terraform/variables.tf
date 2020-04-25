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
  description = "Threshold to trigger an autoscale-in activity, represented as number of messages in SQS"
  type        = number
}

variable "worker_scale_out_threshold" {
  description = "Threshold to trigger an autoscale-in activity, represented as number of messages in SQS"
  type        = number
}

variable "worker_scale_in_cooldown" {
  description = "Cooldown, in seconds, between triggering scale in events"
  type        = number
}

variable "worker_scale_out_cooldown" {
  description = "Cooldown, in seconds, between triggering scale out events"
  type        = number
}

variable "worker_min_capacity" {
  description = "worker service minimum number of autoscaled containers"
  default     = 1
}

variable "worker_max_capacity" {
  description = "worker service minimum number of autoscaled containers"
}
