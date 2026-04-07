# dbt project

This folder contains the dbt project used by the Terraform-managed Databricks job in the repository root.

## Project details

- Project name: `dtb_databricks_IAC`
- dbt profile name: `dtb_databricks_IAC`
- Main model folders: `models/bronze`, `models/silver`, `models/example`

## First-time local setup

Install dbt with the Databricks adapter:

```bash
pip install dbt-databricks
```

Create `~/.dbt/profiles.yml` with a matching profile name:

```yaml
dtb_databricks_IAC:
  target: dev
  outputs:
    dev:
      type: databricks
      host: https://adb-xxxxxxxxxxxxxxxx.x.azuredatabricks.net
      http_path: /sql/1.0/warehouses/<warehouse-id>
      token: <databricks-pat>
      catalog: workspace
      schema: dbt_kaliche
      threads: 4
```

Then run:

```bash
dbt deps
dbt debug
dbt build
```

## Source dependencies

This project currently expects these source tables to exist:

- `transform.sql_db.sales_bronze`
- `samples.bakehouse.sales_customers`
- `samples.bakehouse.sales_transactions`

If your environment uses different catalogs or schemas, update the source YAML files before running `dbt build`.

## Output location

Seeds in this project are configured to use:

- Catalog: `workspace`
- Schema: `dbt_kaliche`

Adjust that in `dbt_project.yml` if your target schema should be different.
