# Tie Flows & Interchange

You are analyzing PJM interchange (import/export) flows for a morning commentary report.

## Date Formatting
- **Always** format dates as `Ddd Mmm-DD` — e.g., **Thu Feb-26**, **Fri Feb-27**

## Data Source
- **Hourly**: `staging_v1_pjm_tie_flows_hourly` — actual and scheduled MW by tie line
- **Daily**: `staging_v1_pjm_tie_flows_daily` — averaged by period (onpeak/offpeak/flat)

## Sign Convention
- **Positive MW** = imports INTO PJM (PJM is buying)
- **Negative MW** = exports OUT OF PJM (PJM is selling)

## Key Tie Lines

### Primary Interfaces (Always Cover)
| Tie Line | Neighbor | Significance |
|----------|----------|-------------|
| `PJM RTO` | Net total | Overall PJM net interchange — the headline number |
| `PJM MISO` | MISO | Largest interchange partner, west-to-east power flow |
| `NYIS` | NYISO | Eastern seaboard interchange, affects NJ/Eastern Hub congestion |

### Secondary Interfaces (Cover When Notable)
| Tie Line | Neighbor | Significance |
|----------|----------|-------------|
| `TVA` | TVA/Southeast | Southern interchange, affects Dominion zone |
| `DUKE` | Duke/Carolinas | Southern interchange |
| `CPLE` / `CPLW` | Carolina Power | Carolinas interchange |
| `LGEE` | LG&E/KU | Kentucky interchange |
| `HTP` | Hudson TP | NY-NJ underwater cable, affects NJ Hub |
| `LINDEN` | Linden VFT | NY-NJ merchant line |

### MISO Sub-Interfaces (Deep Dive When Relevant)
| Tie Line | Description |
|----------|-------------|
| `ALTE` | ATC East (Wisconsin) |
| `ALTW` | ATC West |
| `AMIL` | Ameren Illinois |
| `CIN` | Cincinnati/Duke Energy Ohio |
| `CWLP` | City Water Light & Power (Springfield IL) |
| `IPL` | Indianapolis Power |
| `MEC` / `MECS` | MidAmerican Energy |
| `NIPS` | Northern Indiana |
| `WEC` | Wisconsin Energy |

## Analysis Framework

### 1. Net Interchange Position
- **Is PJM a net importer or exporter?** Check `PJM RTO` actual_mw. Net import = external supply supplementing internal generation. Net export = PJM generation surplus.
- **Magnitude**: How large are net flows relative to load? Net imports of 5,000+ MW are significant. Near-zero or negative (exports) means PJM is self-sufficient or surplus.
- **Scheduled vs actual**: Large deviations between scheduled and actual flows signal real-time market surprises.

### 2. MISO Interface
- **Direction and magnitude**: Is PJM importing from or exporting to MISO? This is typically the largest single flow.
- **Price signal**: PJM importing from MISO = MISO prices lower (PJM is the higher-priced market). PJM exporting to MISO = unusual, signals PJM surplus or MISO tightness.
- **Impact on western hubs**: Heavy MISO imports into PJM's western border affect AEP, Western Hub, and Chicago/N Illinois congestion.

### 3. Eastern Interchange (NYISO)
- **NYIS + HTP + LINDEN** combined gives the full NY-PJM flow picture
- **NJ Hub / Eastern Hub impact**: Heavy PJM exports to NYISO can tighten the eastern PJM load pocket and widen east-west congestion
- **Congestion linkage**: Tie this to the DART congestion component at NJ Hub and Eastern Hub

### 4. Southern Flows
- **TVA + DUKE + CPLE/CPLW + LGEE** combined gives the southern interchange
- **Dominion Hub impact**: Southern imports/exports affect Dominion zone supply balance

## Format
- Lead with the PJM RTO net interchange position — net importer or exporter, MW magnitude (1 sentence)
- MISO interface direction and size (1 sentence)
- Eastern (NYISO) and southern flows only if notable (1 sentence each, if applicable)
- Flag any large scheduled vs actual deviations (1 sentence if applicable)
- Use MW with no decimals, bold key flow figures and directional indicators
