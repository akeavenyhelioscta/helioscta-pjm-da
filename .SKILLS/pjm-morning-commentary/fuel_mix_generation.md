# Fuel Mix & Generation Stack

You are analyzing PJM generation dispatch and fuel mix for a morning commentary report.

## Date Formatting
- **Always** format dates as `Ddd Mmm-DD` — e.g., **Thu Feb-26**, **Fri Feb-27**

## Data Source
- **Hourly**: `staging_v1_pjm_fuel_mix_hourly` — generation output by fuel type (MW)
- **Daily**: `staging_v1_pjm_fuel_mix_daily` — averaged by period (onpeak/offpeak/flat)

## Available Fuel Categories
| Column | Description |
|--------|-------------|
| `gas` | Natural gas-fired generation |
| `coal` | Coal-fired generation |
| `nuclear` | Nuclear baseload |
| `wind` | Wind generation |
| `solar` | Solar generation (grid-scale) |
| `hydro` | Hydroelectric |
| `oil` | Oil/petroleum-fired (peakers) |
| `storage` | Battery storage dispatch |
| `multiple_fuels` | Dual-fuel units |
| `other_renewables` | Biomass, landfill gas, etc. |
| `other` | Uncategorized |
| `total` | Total system generation |
| `thermal` | Gas + coal + oil + multiple_fuels |
| `renewables` | Wind + solar + hydro + other_renewables |
| `gas_pct_thermal` | Gas as % of thermal dispatch |
| `coal_pct_thermal` | Coal as % of thermal dispatch |

## Analysis Framework

### 1. Thermal Dispatch (Primary Focus)
- **Gas vs coal share**: What % of thermal dispatch is gas (`gas_pct_thermal`) vs coal (`coal_pct_thermal`)? This is the key marginal cost indicator.
- **Gas generation level**: Absolute MW of gas burn — higher gas burn = higher marginal clearing prices, more sensitivity to gas price swings
- **Coal displacement**: Is gas displacing coal (gas share rising) or coal picking up load (gas share falling)? This signals the heat rate spread and fuel switching economics.
- **Peak vs offpeak dispatch shift**: Does the gas/coal mix change materially between peak and offpeak hours?

### 2. Baseload & Renewables
- **Nuclear output**: Is nuclear running at full capacity (~30,000 MW) or are there outages reducing baseload? Nuclear dips are bullish for gas burn and prices.
- **Wind generation**: Actual wind output vs typical levels. High wind displaces gas and depresses prices. Note if wind drops off during peak hours.
- **Solar generation**: Solar shape during HE 9-17 — does it meaningfully reduce peak-hour gas burn?
- **Net load** (total load minus renewables): This is what thermal units must serve. Rising net load = more gas burn = higher prices.

### 3. Supply-Side Price Signals
- **Total generation vs load**: Generation should track load closely. Large gaps suggest imports/exports or storage dispatch.
- **Oil/peaker dispatch**: Any oil-fired generation signals tight supply conditions and extreme pricing.
- **Storage patterns**: Is storage charging (offpeak) and discharging (peak) as expected? Net storage dispatch during peak hours.

## Format
- Lead with the gas/coal thermal split and direction (1-2 sentences) — this is the most price-relevant signal
- Note nuclear and renewables contribution (1 sentence)
- Flag any anomalies: oil dispatch, unusual wind/solar, nuclear outages (1-2 sentences if applicable)
- Use MW with no decimals for generation levels, percentages with 0 decimals for fuel shares
- Bold the gas % of thermal and any notable generation levels
