# Power BI — Connecting to the Stock ETL Warehouse

This guide connects Power BI Desktop to the Azure SQL warehouse and builds a
starter report page over the star schema.

## Prerequisites

- Power BI Desktop (free, Microsoft Store)
- The Azure SQL database provisioned via `scripts/provision_azure.ps1`
- Your client IP allowed in the SQL server firewall (the provisioning script
  adds it; re-run the firewall step if your IP changed)

## 1. Connect

1. Power BI Desktop → **Get Data** → **Azure** → **Azure SQL Database**
2. Server: `<your-server>.database.windows.net`, Database: `stockmarket`
3. Data Connectivity mode: **Import** (fastest for this data volume; the
   dataset refreshes on demand or on a schedule in the Power BI service)
4. Sign in with **Database credentials** — the `etladmin` user from provisioning
5. Select these tables and click **Load**:
   - `fact_stock_prices`
   - `dim_company`
   - `dim_date`
   - `pipeline_runs`

## 2. Model Relationships

Open the **Model** view. Create (or verify Power BI auto-detected):

| From | To | Cardinality | Direction |
|---|---|---|---|
| `fact_stock_prices[company_id]` | `dim_company[company_id]` | Many-to-one | Single |
| `fact_stock_prices[date_id]` | `dim_date[date_id]` | Many-to-one | Single |

Mark `dim_date` as the model's date table:
**Table tools → Mark as date table → `full_date`**.

## 3. DAX Measures

Create these on `fact_stock_prices` (**Modeling → New measure**):

```dax
Latest Close =
VAR MaxDate = MAX ( fact_stock_prices[price_date] )
RETURN
    CALCULATE (
        AVERAGE ( fact_stock_prices[close_price] ),
        fact_stock_prices[price_date] = MaxDate
    )
```

```dax
30-Day Return % =
VAR LatestDate = MAX ( fact_stock_prices[price_date] )
VAR PriorDate = LatestDate - 30
VAR LatestClose =
    CALCULATE (
        AVERAGE ( fact_stock_prices[close_price] ),
        fact_stock_prices[price_date] = LatestDate
    )
VAR PriorClose =
    CALCULATE (
        AVERAGE ( fact_stock_prices[close_price] ),
        FILTER (
            ALL ( fact_stock_prices[price_date] ),
            fact_stock_prices[price_date] <= PriorDate
        ),
        LASTDATE ( fact_stock_prices[price_date] )
    )
RETURN
    DIVIDE ( LatestClose - PriorClose, PriorClose )
```

```dax
Load Success Rate % =
DIVIDE (
    CALCULATE ( COUNTROWS ( pipeline_runs ), pipeline_runs[status] = "SUCCESS" ),
    COUNTROWS ( pipeline_runs )
)
```

Format the two percentage measures as **Percentage** with 2 decimals.

## 4. Starter Report Page

Four visuals, one page:

1. **Price trend (line chart)** — Axis: `dim_date[full_date]`; Values:
   average of `close_price`; Legend: `dim_company[company_name]`.
   This is the headline visual — give it the top half of the page.
2. **Daily return distribution (histogram)** — a clustered column chart with
   `daily_return` bucketed into bins (right-click the field → New group →
   Bin size 0.005). Compare tickers with `dim_company[ticker]` as legend.
3. **Volume by month (matrix heat map)** — Rows: `dim_company[ticker]`;
   Columns: `dim_date[month_name]` (sort by `dim_date[month]`); Values: sum of
   `volume`; conditional formatting → background color scale.
4. **Pipeline health (table)** — `pipeline_runs[started_at]`, `[status]`,
   `[rows_inserted]`, `[error_message]`, sorted newest first, plus the
   **Load Success Rate %** measure as a card beside it.

Add a **slicer** on `dim_company[company_name]` and one on
`dim_date[year]` — both filter the whole page.

## 5. Refresh

- Desktop: **Home → Refresh** pulls the latest rows after each pipeline run.
- Power BI service (optional): publish the report, then configure a scheduled
  refresh with the same database credentials — daily at ~23:30 UTC, one hour
  after the pipeline's timer.

## Troubleshooting

- **"Cannot connect to server"** — your IP is not in the SQL firewall. Re-run
  the `AllowMyIP` firewall rule step in `scripts/provision_azure.ps1`.
- **Login failed** — the serverless database auto-pauses when idle; the first
  connection after a pause takes ~30 seconds to resume. Retry once.
- **Slow first load** — same auto-pause resume delay; subsequent Imports are fast.
