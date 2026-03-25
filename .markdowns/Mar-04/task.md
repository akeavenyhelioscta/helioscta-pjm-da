 Implement UI/plot improvements for forecast charts with minimal scope and no data logic regressions.

  Constraints:
  - Keep existing data fetching/transformation behavior unless absolutely required.
  - Reuse existing chart utilities/components where possible.                                                    
  - Follow current project conventions and styling patterns.                                                     
  - Do not introduce unrelated refactors.                                                                        
                                                                                                                 
  Required changes:                                                                                              
                                                                                                                 
  1) Forecast window label format                                                                                
  - Update forecast window display text to:                                                                      
    Start Date: ddd mmm-dd                                                                                       
    End Date: ddd mmm-dd                                                                                         
  - Example: Start Date: Mon Jan-06, End Date: Thu Jan-09                                                        
                                                                                                                 
  2) Hover date formatting                                                                                       
  - In every hover template across all relevant PJM load forecast plots, format dates as:                        
    ddd mmm-dd                                                                                                   
  - Ensure consistency for all traces and all plots.                                                             
                                                                                                                 
  3) Per-plot forecast toggles                                                                                   
  - Add/enable independent forecast-series toggles within each individual plot.                                  
  - Toggling in one plot must not affect forecast visibility in other plots.                                     
  - Preserve behavior of non-forecast series.                                                                    
                                                                                                                 
  4) One plot per row layout                                                                                     
  - Update chart layout so each plot spans a full row (single-column stack).                                     
  - Maintain responsive behavior on desktop and mobile.                                                          
                                                                                                                 
  5) Weekend shading                                                                                             
  - Add subtle weekend shading (Saturday/Sunday) to each plot over visible x-range.                              
  - Shading must improve readability and not hide lines/markers.                                                 
                                                                                                                 
  Implementation guidance:                                                                                       
  - Prefer plot-native shapes/overlays for weekend shading.                                                      
  - Centralize date-format function if one already exists; otherwise add a small reusable helper.                
  - Keep hover formatting and forecast window formatting aligned to the same format rule.                        
                                                                                                                 
  Files:                                                                                                         
  - Locate and edit only files directly responsible for:                                                         
    - PJM Load Forecast page rendering                                                                           
    - chart config/layout                                                                                        
    - hover templates / date formatting helpers                                                                  
    - forecast legend/toggle behavior                                                                            
  - If multiple candidate files exist, choose the smallest-impact set.                                           
                                                                                                                 
  Validation steps:                                                                                              
  1) Run lint/type checks for touched files (or project-standard lint/typecheck command).                        
  2) Confirm no type errors introduced.                                                                          
  3) Verify behavior manually in code by checking:                                                               
     - forecast window text format                                                                               
     - hover date format on all plots/traces                                                                     
     - independent toggles per plot                                                                              
     - one-plot-per-row layout                                                                                   
     - weekend shading present for each plot                                                                     
                                                                                                                 
  Output format (must follow exactly):                                                                           
  1) Summary (3-6 bullets)                                                                                       
  2) Files changed (with brief reason per file)                                                                  
  3) Behavior checklist                                                                                          
     - [x]/[ ] Forecast window format updated                                                                    
     - [x]/[ ] Hover dates standardized                                                                          
     - [x]/[ ] Per-plot forecast toggles work independently                                                      
     - [x]/[ ] One plot per row layout                                                                           
     - [x]/[ ] Weekend shading added                                                                             
  4) Validation run results (commands + key output)                                                              
  5) Risks / follow-ups (if any)                                                                                 
                                                                                                                 
  Definition of done:                                                                                            
  - All five required changes are implemented.                                                                   
  - Lint/type checks pass for touched scope.                                                                     
  - No unrelated code changes. 