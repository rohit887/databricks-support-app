# Databricks Support-Ticket App

A single-file Streamlit app for [Databricks Apps](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/),
backed by **Lakebase Autoscaling** (managed Postgres 17). It lists support
tickets, filters them by status, shows per-ticket message threads, and lets you
create tickets, post messages, and change status.

Postgres is the only source of truth — nothing is cached in the app, so a
browser refresh always re-reads from the database.

## How authentication works

Databricks Apps injects `PGHOST`, `PGDATABASE`, `PGUSER`, and `PGPORT` into the
app's environment when a Lakebase database resource is bound. The **password is
not injected**. Instead the app mints a short-lived OAuth database credential on
every connection:

```python
w = WorkspaceClient()  # reads auth from the environment
credential = w.postgres.generate_database_credential(endpoint=ENDPOINT_NAME)
# credential.token is used as the Postgres password
```

`WorkspaceClient()` takes no arguments — it authenticates as the app's service
principal when deployed, or as your user account when run locally. These tokens
expire after ~1 hour, so the app generates a fresh one per connection and never
caches it. TLS is required (`sslmode="require"`).

## Setup order

Do these steps in order. Note the timing quirk in step 5.

1. **Create the app.** Create a Databricks App and bind your Lakebase database
   resource to it.
2. **Copy `DATABRICKS_CLIENT_ID`.** From the app's environment, copy the
   `DATABRICKS_CLIENT_ID` — this is the app's service principal. You'll grant
   this principal database access next.
3. **Create the Postgres role + grants.** In your Lakebase Postgres database,
   create a role for that service principal (the `DATABRICKS_CLIENT_ID`) and
   grant it the privileges it needs, for example:

   ```sql
   -- Run as a Postgres admin. Replace <DATABRICKS_CLIENT_ID> with the value
   -- copied in step 2.
   CREATE ROLE "<DATABRICKS_CLIENT_ID>" LOGIN;
   GRANT CONNECT ON DATABASE <your_db> TO "<DATABRICKS_CLIENT_ID>";
   GRANT USAGE ON SCHEMA public TO "<DATABRICKS_CLIENT_ID>";
   GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public
       TO "<DATABRICKS_CLIENT_ID>";
   GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public
       TO "<DATABRICKS_CLIENT_ID>";
   ```

4. **Run `schema.sql`.** Apply `schema.sql` to your database (via `psql` or the
   Databricks SQL editor). It creates the `tickets` and `ticket_messages`
   tables plus seed data. This file is for manual use — the app never runs it.
5. **Set `ENDPOINT_NAME`.** Get the endpoint resource name from your branch's
   **Computes** tab (click **Get ID** for your compute, then **Copy resource
   name**). It looks like
   `projects/<project-id>/branches/<branch-id>/endpoints/<endpoint-id>`. Paste
   it into `app.yaml`.

   > **Timing note:** the `PG*` environment variables only appear **after the
   > first deploy** with the Lakebase resource bound. If you deploy and see an
   > error naming `PGHOST` (etc.), redeploy — the variables are injected on the
   > next deploy.

6. **Deploy.** Deploy the app. It will read the `PG*` vars, mint an OAuth token
   with `ENDPOINT_NAME`, connect over TLS, and serve the UI.

## Files

| File               | Purpose                                                        |
| ------------------ | -------------------------------------------------------------- |
| `app.py`           | The entire app (connection layer + UI).                        |
| `app.yaml`         | Databricks Apps launch command and `ENDPOINT_NAME` env var.    |
| `requirements.txt` | `streamlit`, `psycopg[binary]`, `databricks-sdk`.              |
| `schema.sql`       | Reference DDL + seed data. Run manually; the app never uses it.|

## Running locally

With the Databricks SDK configured for your user (e.g. `DATABRICKS_HOST` +
`DATABRICKS_TOKEN`, or `databricks auth login`), export the connection
variables and run:

```bash
export PGHOST=... PGDATABASE=... PGUSER=... PGPORT=5432
export ENDPOINT_NAME='projects/<project-id>/branches/<branch-id>/endpoints/<endpoint-id>'
pip install -r requirements.txt
streamlit run app.py
```
