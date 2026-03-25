You are a senior frontend engineer working in this codebase. Redesign the PJM Load Forecasts page UX/UI while   
  keeping existing behavior stable unless explicitly changed below.                                               
                                                                                                                  
  Primary requirements:                                                                                           
  1. Add **global forecast-date quick filter buttons** that apply to **all plots at once**.                       
  2. Forecast-date filters must support **multi-select** (including “select all” and “clear all” behavior).       
  3. Add preset buttons next to the date filters, including:                                                      
     - Latest
     - 12hr Prior
     - (and similar relative options, e.g., 24hr Prior if available in data)
  4. For load forecast rendering, **cut off data before 9:00 AM** on the forecast date.
  5. Weekend shading must be calculated using **America/New_York (EST/EDT) local date on the forecast date**, not 
  UTC.

  Implementation constraints:
  - Use one shared filter state so every chart stays in sync.
  - Preserve current charting/data pipeline unless a change is required for these features.
  - Handle edge cases (missing prior runs, sparse timestamps, DST transitions).
  - Keep interactions fast and obvious (clear selected state for buttons).

  What to return:
  1. The code changes.
  2. A short explanation of what changed and why.
  3. Any assumptions made (especially around “Latest” and “12hr Prior” definitions).
  4. A quick validation checklist for timezone correctness and chart synchronization.


  Now I want to investigate the following Issues:
  - The Latest Forecast should include a full 7 days. Why isn't Sunday populating?
  - I want the the X axis to disaply Forecast Date and Hour Ending clearly as ddd mmm-dd HH AM/PM
  - I want to room the slider button on the bottom of the plot and the reset zoom buttons