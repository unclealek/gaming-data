variable "databricks_host" {
  type        = string
  description = "Databricks workspace URL, for example https://adb-1234567890123456.7.azuredatabricks.net"
}

variable "databricks_token" {
  type        = string
  description = "Databricks personal access token"
  sensitive   = true
}

variable "warehouse_id" {
  type        = string
  description = "Existing Databricks SQL warehouse ID"
}

variable "job_name" {
  type        = string
  description = "Name of the Databricks job"
  default     = "gaming_data_dbt_pipeline"
}

variable "git_repo_url" {
  type        = string
  description = "Git repository URL for the dbt project source"
  default     = "https://github.com/unclealek/gaming-data.git"
}

variable "git_branch" {
  type        = string
  description = "Git branch Databricks should use for the job"
  default     = "main"
}

variable "dbt_project_directory" {
  type        = string
  description = "Directory of the dbt project inside the Git repository"
  default     = "dtb_databricks_IAC"
}

variable "spark_version" {
  type        = string
  description = "Databricks runtime version for the job cluster"
  default     = "13.3.x-scala2.12"
}

variable "node_type_id" {
  type        = string
  description = "Node type for the Databricks job cluster"
  default     = "i3.xlarge"
}

variable "num_workers" {
  type        = number
  description = "Number of workers for the Databricks job cluster"
  default     = 1
}

variable "job_timezone" {
  type        = string
  description = "Timezone for the job schedule"
  default     = "Europe/Helsinki"
}

variable "job_cron_expression" {
  type        = string
  description = "Cron expression for the job schedule"
  default     = "0 0 9 * * ?"
}

variable "job_pause_status" {
  type        = string
  description = "Schedule state for the Databricks job. Valid values are PAUSED or UNPAUSED."
  default     = "PAUSED"

  validation {
    condition     = contains(["PAUSED", "UNPAUSED"], var.job_pause_status)
    error_message = "job_pause_status must be either PAUSED or UNPAUSED."
  }
}
