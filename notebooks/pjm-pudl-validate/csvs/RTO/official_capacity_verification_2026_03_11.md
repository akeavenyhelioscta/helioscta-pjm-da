# Official Capacity Verification

Full table: `C:\Users\AidanKeaveny\Documents\github\helioscta-pjm-da\notebooks\pjm-pudl-validate\csvs\RTO\official_capacity_verification_2026_03_11.csv`

## Source Notes

- Output rows: 4,322. Verified rows: 3,003. Verified but capacity-discrepant rows: 62. Unverified rows: 1,319.
- Methodology: repeated identical workbook rows were collapsed into a single input record unless the repeated count matched the number of distinct official EIA generators with the same plant/fuel/capacity signature; those cases were re-expanded to generator-level rows.
- Naming ambiguities: many unresolved records are portfolio-style solar or storage projects whose workbook names do not exactly match EIA plant names. These remain `unverified` rather than being force-matched.
- Conflicting capacity values: when 2024 EIA-860 and January 2026 EIA-860M differed, the January 2026 EIA-860M value was treated as the more authoritative current record and the older 2024 value was noted in the row notes.
- Capacity discrepancies: a large share of solar, wind, and storage records match an official plant/unit name but not the workbook MW. Where the plant/unit match is still unique, those rows are marked `verified - discrepancy`; the lower workbook MW may reflect derated, accredited, or modeled capability rather than installed EIA capacity (inference).
- Missing or unavailable official documentation: records with no confident EIA exact-name match remain `unverified`. Additional owner/operator filings, PJM documents, or state records would be needed to push coverage higher for those names.

Naming ambiguity examples:
- 8point3 Macys Maryland PV Portfolio
- AC Power Delanco PV Plant Phase I
- AC Power and Citrine Power Hopatcong PV Plant
- ACCC Mays Landing & Cape May County PV Portfolio
- AEDG Orchard PV Plant
- AEP Clyde PV Plant
- AEP Martinsville Energy Storage Project
- AEP Milton II Energy Storage Project
- AEP Ohio CERTS Microgrid Test Bed Demonstration Project
- AEP Ohio Columbus Energy Storage Project
- AEP South River Energy Storage Project
- AES Corp Pennsylvania Advancion Applications Energy Storage Project
- AMP Bowling Green Wind Farm
- APV Virginia Energy Storage Project
- Air Products Allentown PV Plant

Conflicting/discrepant official-capacity examples:
- 12 Applegate Solar LLC: Workbook row(s): summer/winter 1.9/1.9 MW; repeated 1x. EIA totals for matched unit set: nameplate/summer/winter 1.5/1.5/1.5 MW. Plant and fuel match are unique in EIA, but workbook capacity does not equal the official EIA capacity fields. EIA 860M Operating sheet row(s): row 17402. Workbook capacity appears lower than EIA installed capability; this may reflect a derated or accredited model input rather than installed capacity (inference).
- 24 Applegate Solar LLC: Workbook row(s): summer/winter 4.9/4.9 MW; repeated 1x. EIA totals for matched unit set: nameplate/summer/winter 2.6/2.6/2.6 MW. Plant and fuel match are unique in EIA, but workbook capacity does not equal the official EIA capacity fields. EIA 860M Operating sheet row(s): row 17397. Workbook capacity appears lower than EIA installed capability; this may reflect a derated or accredited model input rather than installed capacity (inference).
- 4 Applegate Solar LLC: Workbook row(s): summer/winter 1.8/1.8 MW; repeated 1x. EIA totals for matched unit set: nameplate/summer/winter 1.4/1.4/1.4 MW. Plant and fuel match are unique in EIA, but workbook capacity does not equal the official EIA capacity fields. EIA 860M Operating sheet row(s): row 17401. Workbook capacity appears lower than EIA installed capability; this may reflect a derated or accredited model input rather than installed capacity (inference).
- Albemarle Beach Solar: Workbook row(s): summer/winter 80/80 MW; repeated 1x. EIA totals for matched unit set: nameplate/summer/winter 130.2/140/140 MW. Plant and fuel match are unique in EIA, but workbook capacity does not equal the official EIA capacity fields. EIA 860M Operating sheet row(s): row 22688. Workbook capacity appears lower than EIA installed capability; this may reflect a derated or accredited model input rather than installed capacity (inference).
- Atlantic Coast Freezers Solar Facility: Workbook row(s): summer/winter 2.2/2.2 MW; repeated 1x. EIA totals for matched unit set: nameplate/summer/winter 2/2/2 MW. Plant and fuel match are unique in EIA, but workbook capacity does not equal the official EIA capacity fields. EIA 860M Operating sheet row(s): row 16172. Workbook capacity appears lower than EIA installed capability; this may reflect a derated or accredited model input rather than installed capacity (inference).
- Barrette Outdoor Living, Inc.: Workbook row(s): summer/winter 1.5/0.2 MW; repeated 1x. EIA totals for matched unit set: nameplate/summer/winter 1.9/1.9/0.7 MW. Plant and fuel match are unique in EIA, but workbook capacity does not equal the official EIA capacity fields. EIA 860M Operating sheet row(s): row 21735. Workbook capacity appears lower than EIA installed capability; this may reflect a derated or accredited model input rather than installed capacity (inference).
- Bernards Solar: Workbook row(s): summer/winter 2.6/2.6 MW; repeated 1x. EIA totals for matched unit set: nameplate/summer/winter 2.9/2.9/2.9 MW. Plant and fuel match are unique in EIA, but workbook capacity does not equal the official EIA capacity fields. EIA 860M Operating sheet row(s): row 18532. Workbook capacity appears lower than EIA installed capability; this may reflect a derated or accredited model input rather than installed capacity (inference).
- Berry Plastics Solar: Workbook row(s): summer/winter 10.1/10.1 MW; repeated 1x. EIA totals for matched unit set: nameplate/summer/winter 9.8/9.8/9.8 MW. Plant and fuel match are unique in EIA, but workbook capacity does not equal the official EIA capacity fields. EIA 860M Operating sheet row(s): row 17163. Workbook capacity appears lower than EIA installed capability; this may reflect a derated or accredited model input rather than installed capacity (inference).
- Cinnamon Bay Edgeboro Landfill: Workbook row(s): summer/winter 9.1/9.1 MW; repeated 1x. EIA totals for matched unit set: nameplate/summer/winter 9.6/6/6 MW. Plant and fuel match are unique in EIA, but workbook capacity does not equal the official EIA capacity fields. EIA 860M Operating sheet row(s): row 15960.
- Clyde Peaking Engine: Workbook row(s): summer/winter 9.4/9.4 MW; repeated 1x. EIA totals for matched unit set: nameplate/summer/winter 10/10/10 MW. Plant and fuel match are unique in EIA, but workbook capacity does not equal the official EIA capacity fields. EIA 860M Operating sheet row(s): row 22679.
- Covanta Delaware Valley: Workbook row(s): summer/winter 83/83 MW; repeated 1x. EIA totals for matched unit set: nameplate/summer/winter 90/80/80 MW. Plant and fuel match are unique in EIA, but workbook capacity does not equal the official EIA capacity fields. EIA 860M Operating sheet row(s): row 8881.
- DC Water Solar: Workbook row(s): summer/winter 4.5/4.5 MW; repeated 1x. EIA totals for matched unit set: nameplate/summer/winter 3.5/3.5/3.5 MW. Plant and fuel match are unique in EIA, but workbook capacity does not equal the official EIA capacity fields. EIA 860M Operating sheet row(s): row 22430. Workbook capacity appears lower than EIA installed capability; this may reflect a derated or accredited model input rather than installed capacity (inference).
- Engelhard Solar LLC: Workbook row(s): summer/winter 1.1/1.1 MW; repeated 1x. EIA totals for matched unit set: nameplate/summer/winter 0.8/0.8/0.8 MW. Plant and fuel match are unique in EIA, but workbook capacity does not equal the official EIA capacity fields. EIA 860M Operating sheet row(s): row 17399. Workbook capacity appears lower than EIA installed capability; this may reflect a derated or accredited model input rather than installed capacity (inference).
- FedEx Woodbridge: Workbook row(s): summer/winter 2.4/2.4 MW; repeated 1x. EIA totals for matched unit set: nameplate/summer/winter 2/2/1 MW. Plant and fuel match are unique in EIA, but workbook capacity does not equal the official EIA capacity fields. EIA 860M Operating sheet row(s): row 15344. Workbook capacity appears lower than EIA installed capability; this may reflect a derated or accredited model input rather than installed capacity (inference).
- Francis Scott Key Mall: Workbook row(s): summer/winter 2.1/2.1 MW; repeated 1x. EIA totals for matched unit set: nameplate/summer/winter 1.6/1.6/1.6 MW. Plant and fuel match are unique in EIA, but workbook capacity does not equal the official EIA capacity fields. EIA 860M Operating sheet row(s): row 21690. Workbook capacity appears lower than EIA installed capability; this may reflect a derated or accredited model input rather than installed capacity (inference).

## Official Sources Used

- U.S. Energy Information Administration (EIA), *Inventory of Operating Generators as of January 2026* and companion sheets in January 2026 EIA-860M workbook. URL: https://www.eia.gov/electricity/data/eia860m/xls/january_generator2026.xlsx
- U.S. Energy Information Administration (EIA), *2024 Form EIA-860 Data - Schedule 3, 'Generator Data' (Operable Units Only)* and companion sheets in the 2024 Form EIA-860 data files. URL: https://www.eia.gov/electricity/data/eia860/xls/eia8602024.zip
- U.S. Energy Information Administration (EIA), *2024 Form EIA-860 Data - Schedule 4, 'Generator Ownership' (Jointly or Third-Party Owned Only)*. URL: https://www.eia.gov/electricity/data/eia860/xls/eia8602024.zip