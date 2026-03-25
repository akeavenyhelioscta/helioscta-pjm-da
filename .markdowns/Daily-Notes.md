# Tues Feb 3rd

**TASK: Backend Data Manipulation or DBT Views**

I want to investigate the best way to format data either in DBT or in my backend/. My backend folder for PJM is currently in this repo 'C:\Users\AidanKeaveny\Documents\github\helioscta-backend'. I create DBT views for PJM here C:\Users\AidanKeaveny\Documents\github\helioscta-backend\backend\dbt\dbt_azure_postgresql\models\power\dbt_pjm_v1_2026_feb_19. How can this be improved?

**TASK: Migration of PJM STACK MODEL from EXCEL to BACKEND and FRONT-END**

I want to create a front-end dashboard for my PJM STACK MODEL this will migrate away from an excel file listed in @.SKILLS\stack-model\reference\PJM_Stack_Model_v3.xlsx. I want this documented in one Markdown file for now in @.SKILLS\stack-model\implementation_plan.md.

- Agent 1: Will investage the data sources in this excel file. I want each data source to be called by my backend api routes. I will need to create a table for the 'PJM Raw Data' sheets.
- Agent 2: Will investigate how we pull regional forecasts from this schema from Postgres db for PJM `from meteologica.usa_pjm_`. Will it be better to create a dbt model to view these regional forecasts.

# (2026-02-26) Thurs Feb 26th

I want to create a markdown file that will be generated every morning for morning commentary:
- Teammate 1: Will inpsect the markdown files that I have already generated here .skills\pjm-morning-commentary
- Teamate 2: Will Inpsect the data I have already made avaliable in this schema in the database `dbt_pjm_v1_2026_feb_19`. And will expand the markdown preferences for data sources 
- Teamate 3: Will planning how to improve this overtime. Each day will produce a new markdown file generated in morning-report formatted as (YYYY-MM-DD) DDD MMM-DD.md

I want to create a frontend page to quickly view across PJM RTO, Mid Atl, South and Western how the load forecast is evolving and how it has performed against actual load (PJM Load RT Prelim). 
- Teammate 1: Will inpsect how load is scraped from PJM Data Miner here @C:\Users\AidanKeaveny\Documents\github\helioscta-backend\backend\src\power\pjm\seven_day_load_forecast_v1_2025_08_13.py and how the views are created here C:\Users\AidanKeaveny\Documents\github\helioscta-backend\backend\dbt\dbt_azure_postgresql\models\power\dbt_pjm_v1_2026_feb_19.
- Teamate 2: Will Inpsect the view I have already create for PJM Hourly Load Forecast here frontend\app\api\pjm-like-day-forecast
- Teamate 3: Will implement a plan on how to do this in the front-end. This task is two fold as I want to view how the actuals vs forecast preformed (default to yesterday) and how the forecast is evolving. I want to scan this across all 4 regions.

# I place the highest emphasis on the load forecast around 7-8am MST for DA LMP predictions

# (2026-02-25) Wed Feb 25th

- Do you back out lmp for days most similar to load

## DA-MODEL

### Creating the da-model

I want to create a day-ahead model for predicting a probabilist outcome for lmp prices in PJM for Western Hub:
- Teammate 1: Will research papers and existing github repos online. I've started with some papers located here da-model\research\pdf-reports and links to git repos here @da-model\research\github_repos.md
- Teamate 2: Will Inpsect the data I have already made avaliable in this schema in the database `dbt_pjm_v1_2026_feb_19`. And will outline a plan for which data sources need to be made avaliable from this schema `pjm`
- Teamate 3: Will planning how to implment this python code in da-model\ and outline the plan in .skills\da-model\

### Investigating performance

I want to investigate @da-model\src\pjm_da_forecast\pipelines\forecast.py:
- Teammate 1: Will investigate why the model missed to the upside using findings from @.skills\da-model\research_summary.md
- Teammate 2: Will investigate if seasonability and lookback period will improve results
- Teammate 3: Will develop a jupyter notebook to investigate model results

## Like-Day Model

# Creating the like-day model

I want to create a like-day model for predicting a probabilist outcome for lmp prices in PJM for Western Hub:
- Teammate 1: Will research papers and existing github repos online. I want these results store in .skills\like-day-model
- Teamate 2: Will Inpsect the source code I have already made avaliable here @backend\src\pjm_like_day\like_day.py
- Teamate 3: Will planning how to implment this python code in like-day-model\ and outline the plan in .skills\like-day-model\

I want to investigate @like-day-model\src\pjm_like_day_forecast\pipelines\forecast.py:
- Teammate 1: Will investigate why the model missed to the downside using findings from @.skills\like-day-model\research.md
- Teammate 2: Will investigate if seasonability and lookback period will improve results
- Teammate 3: Will develop a jupyter notebook to investigate model results