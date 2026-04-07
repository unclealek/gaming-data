terraform {
  required_version = ">= 1.0.0"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.112"
    }
  }
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

resource "databricks_job" "dbt_pipeline_job" {
  name = var.job_name

  git_source {
    url      = var.git_repo_url
    provider = "gitHub"
    branch   = var.git_branch
  }

  job_cluster {
    job_cluster_key = "dbt_cluster"
    new_cluster {
      spark_version = var.spark_version
      node_type_id  = var.node_type_id
      num_workers   = var.num_workers
    }
  }

  task {
    task_key = "run_dbt_models"

    dbt_task {
      project_directory = var.dbt_project_directory
      commands          = ["dbt deps", "dbt build"]
      warehouse_id      = var.warehouse_id
    }

    job_cluster_key = "dbt_cluster"
  }

  schedule {
    quartz_cron_expression = var.job_cron_expression
    timezone_id            = var.job_timezone
    pause_status           = var.job_pause_status
  }
}
