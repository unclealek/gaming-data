terraform {
  required_version = ">= 1.0.0"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

resource "databricks_job" "dbt_pipeline_job" {
  name = "gaming_data_dbt_pipeline"

  git_source {
    url      = "https://github.com/unclealek/gaming-data.git"
    provider = "gitHub"
    branch   = "main"
  }

  job_cluster {
    job_cluster_key = "dbt_cluster"
    new_cluster {
      spark_version = "13.3.x-scala2.12"
      node_type_id  = "i3.xlarge"
      num_workers   = 1
    }
  }

  task {
    task_key = "run_dbt_models"

    dbt_task {
      project_directory = "dtb_databricks_IAC"
      commands          = ["dbt deps", "dbt build"]
      warehouse_id      = var.warehouse_id
    }

    job_cluster_key = "dbt_cluster"
  }

  schedule {
    quartz_cron_expression = var.job_cron_expression
    timezone_id            = var.job_timezone
  }
}
