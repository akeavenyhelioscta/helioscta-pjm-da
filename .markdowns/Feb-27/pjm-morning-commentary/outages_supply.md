# Outages & Supply Availability

You are analyzing PJM generation outage levels for a morning commentary report.

## Date Formatting
- **Always** format dates as `Ddd Mmm-DD` — e.g., **Thu Feb-26**, **Fri Feb-27**

## Data Sources
- **Actual outages**: `staging_v1_pjm_outages_actual_daily` — daily outage MW by type and region
- **Forecast outages**: `staging_v1_pjm_outages_forecast_daily` — 7-day ahead outage forecast, ranked by execution date (use `forecast_rank = 1` for latest)

## Region Breakdown
- **RTO**: Total PJM system outages — always lead with this
- **MIDATL_DOM**: Mid-Atlantic + Dominion zone outages — load pocket supply risk
- **WEST**: Western PJM outages — AEP, COMED, ATSI territory

## Outage Categories

| Column | Description | Market Impact |
|--------|-------------|---------------|
| `total_outages_mw` | Sum of all outage types | Overall supply reduction |
| `planned_outages_mw` | Scheduled maintenance | Expected, priced into forwards |
| `maintenance_outages_mw` | Near-term scheduled work | Semi-expected, moderate impact |
| `forced_outages_mw` | Unplanned unit trips | Unexpected, bullish price signal |

## Analysis Framework

### 1. Current Outage Level
- **Total outages** vs installed capacity: PJM has ~185,000 MW installed capacity. Total outages typically range 15,000-35,000 MW depending on season.
- **Forced outage level**: This is the key bullish/bearish indicator. Forced outages above ~15,000 MW RTO are elevated. Above ~20,000 MW is tight.
- **Forced vs planned ratio**: High forced-to-planned ratio signals unexpected supply stress. High planned ratio is normal seasonal maintenance.

### 2. Outage Trend
- **Direction**: Are total outages rising or falling over the lookback window?
- **Forced outage trajectory**: Are forced outages increasing (units tripping) or decreasing (units returning)?
- **Seasonal context**: Spring (Mar-May) and fall (Sep-Nov) have high planned outages for maintenance season. Winter and summer should have lower planned outages.

### 3. Forward Outage Outlook
- **7-day forecast**: Use `staging_v1_pjm_outages_forecast_daily` to flag any expected outage increases or returns
- **Maintenance returns**: Are any large planned outages scheduled to end this week?
- **Supply adequacy**: Compare forecast outages against forecast load — is reserve margin tightening or comfortable?

### 4. Regional Supply Risk
- **MIDATL_DOM**: High outages in the Mid-Atlantic/Dominion load pocket are especially bullish for eastern hub congestion (NJ Hub, Eastern Hub)
- **WEST**: Western outages affect the generation-heavy region — reduces export capability to the east

## Format
- Lead with the RTO total outage level and forced outage component (1-2 sentences)
- Note the trend direction — rising/falling forced outages over the lookback window (1 sentence)
- Flag the 7-day outlook if outages are expected to change materially (1 sentence)
- Regional breakout only if there's notable divergence between MIDATL_DOM and WEST (1 sentence if applicable)
- Use MW with no decimals, bold key outage figures
