<<<<<<< HEAD
1. Business Context

A mobile gaming company called Arcadia Games has launched a free-to-play strategy game called Kingdom Clash.

The game generates revenue through:

In-app purchases (skins, weapons, power-ups)

Season battle passes

Advertisements shown to free players

The game has reached 3 million downloads, but the company faces two problems:

Low player retention after Day 7

Low conversion from free players to paying players

The company wants to build a data platform and analytics layer to better understand player behavior.

2. Business Questions

The leadership team wants answers to questions like:

Player Behavior

How many users return after Day 1, Day 7, Day 30?

At what level do most players quit?

Which countries produce the highest engagement?

Monetization

What percentage of players become paying users?

What is the Average Revenue Per User (ARPU)?

Which items are purchased most frequently?

Gameplay Insights

Which levels are too difficult?

Do players who join guilds/clans stay longer?

Does watching ads increase retention?
=======
# Game Data: dbt + Terraform

This repository contains:

- A dbt project in `dtb_databricks_IAC`
- Terraform that creates a Databricks job to run that dbt project

The intended flow is:

1. Prepare Databricks objects and access
2. Set up and test the dbt project locally
3. Deploy the scheduled dbt job with Terraform

## Repository structure

- `dtb_databricks_IAC/`: dbt project
- `main.tf`: Databricks job definition
- `variables.tf`: Terraform input variables
- `outputs.tf`: Terraform outputs
- `terraform.tfvars.example`: example Terraform variable values

## First-time setup

### 1. Prepare Databricks

Before running dbt or Terraform, make sure you already have:

- A Databricks workspace URL
- A Databricks personal access token
- A Databricks SQL warehouse ID
- Permission to read the source data used by this dbt project
- A Git repository that Databricks can access

This dbt project currently expects these source locations:

- `transform.sql_db.sales_bronze`
- `samples.bakehouse.sales_customers`
- `samples.bakehouse.sales_transactions`

It also writes project outputs under:

- Catalog: `workspace`
- Schema: `dbt_kaliche`

Create or adjust those objects if your environment uses different names.

### 2. Set up dbt for the first time

The dbt project lives in [dtb_databricks_IAC](/Users/kelvinaliche/Desktop/Projects/game-data/dtb_databricks_IAC).

Install dbt with the Databricks adapter:

```bash
pip install dbt-databricks
```

Create a local dbt profile file at `~/.dbt/profiles.yml` using the profile name from this project: `dtb_databricks_IAC`.

Example:

```yaml
dtb_databricks_IAC:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: workspace
      schema: dbt_kaliche
      host: https://adb-xxxxxxxxxxxxxxxx.x.azuredatabricks.net
      http_path: /sql/1.0/warehouses/<warehouse-id>
      token: <databricks-pat>
      threads: 4
```

Then run the initial dbt commands from the repository root:

```bash
cd dtb_databricks_IAC
dbt deps
dbt debug
dbt build
```

If `dbt debug` or `dbt build` fails, fix that before deploying Terraform. Terraform only schedules the dbt job; it does not fix project-level dbt configuration issues.

### 3. Set up Terraform for the first time

From the repository root, copy the example variable file:

```bash
cp terraform.tfvars.example terraform.tfvars
```

Update `terraform.tfvars` with your real values:

- `databricks_host`
- `databricks_token`
- `warehouse_id`

Optional settings you can also change:

- `job_name`
- `git_repo_url`
- `git_branch`
- `dbt_project_directory`
- `spark_version`
- `node_type_id`
- `num_workers`
- `job_timezone`
- `job_cron_expression`
- `job_pause_status`

Then initialize and review the plan:

```bash
terraform init
terraform plan
```

Apply the job:

```bash
terraform apply
```

## What Terraform creates

Terraform currently creates one Databricks job that:

- Pulls this repository from Git
- Uses the configured branch
- Runs inside the `dtb_databricks_IAC` folder
- Executes `dbt deps` and `dbt build`
- Uses the SQL warehouse you provide
- Runs on the Databricks schedule you configure

Terraform does not currently create:

- The Databricks workspace
- The SQL warehouse
- Catalogs, schemas, or source tables
- Secrets or permissions

## Recommended workflow

When making changes:

1. Update and test dbt locally in `dtb_databricks_IAC`
2. Commit and push the repository changes
3. Update Terraform only if job settings changed
4. Run `terraform plan`
5. Run `terraform apply`

## Notes

- `terraform.tfvars` is ignored by Git and should keep your real token out of source control.
- The Terraform schedule defaults to `PAUSED`. Change `job_pause_status` to `UNPAUSED` when you want scheduled runs to start.
- The dbt example models reference the `samples.bakehouse` catalog and schema. If that data is not available in your workspace, remove or update those models before production use.
>>>>>>> d3b6853 (update readme file)
