output "databricks_job_id" {
  description = "ID of the Databricks job created for the dbt pipeline"
  value       = databricks_job.dbt_pipeline_job.id
}

output "databricks_job_url" {
  description = "URL of the Databricks job in the workspace"
  value       = "${var.databricks_host}/jobs/${databricks_job.dbt_pipeline_job.id}"
}
