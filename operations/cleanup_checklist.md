# End-of-trial cleanup and cost checklist

Fill in the date/time and tick each item. Add the screenshot file name from `evidence/` where it applies.

| # | Action | Where in Databricks | Done (date/time) | Evidence |
|---:|---|---|---|---|
| 1 | Check remaining trial credits and days left | Workspace top bar trial banner, or account console (profile menu → **Manage account**) → **Usage** / billing | | `evidence/09_operations/credits_remaining.png` |
| 2 | Stop the SQL warehouse (and lower **Auto stop** to 5–10 min) | **SQL Warehouses** → your warehouse → **Stop** | | `warehouse_stopped.png` |
| 3 | Confirm the job schedule is **Paused** (or there is none) | **Jobs & Pipelines** → job → **Schedules & Triggers** | | `job_schedule_paused.png` |
| 4 | No job runs in progress | **Jobs & Pipelines** → **Runs** | | |
| 5 | Detach / terminate any notebook compute | Notebook compute dropdown → **Detach**. Serverless stops by itself when idle | | |
| 6 | Export dashboard (PDF or screenshot) and keep the link | Dashboard → **Share** / **Download as PDF** (or screenshot) | | `dashboards/` |
| 7 | Export notebooks (already in Git). Also **File → Export → Source file** as a backup | Notebook menu | | |
| 8 | Commit and push every change from the Git folder | Git folder → **Git…** button → **Commit & Push** | | Commit history |
| 9 | Save `.pbix` and the Power BI screenshot into `powerbi/` | Power BI Desktop | | |
| 10 | Revoke the personal access token used for Power BI | Profile → **Settings** → **Developer** → **Access tokens** → **Revoke** | | |
| 11 | No payment method added, no upgrade, no extra cloud resources created | Account console → billing | | Statement in README |

**Credit-saving habits used during the project**
* Serverless notebooks/jobs only, so there was no always-on cluster.
* One small SQL warehouse (the trial allows one), with Auto stop at 10 minutes. Stopped manually after BI work.
* The job ran on demand (Run now). The schedule was only a paused test window, never left active.
* No parallel experiments. Each layer was rebuilt once per change.
