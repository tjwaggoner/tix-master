-- Enter this manually under 'Data' in you dashboard 
SELECT                                                                                                                                                    
      -- Event identifiers                                                                                                                                  
      f.event_sk,                                                                                                                                           
      f.event_id,                                                                                                                                           
      f.event_name,                                                                                                                                         
      f.event_type, 
      round((f.price_max+f.price_min)/2,2) as avg_price,                                                                                                                                        
                                                                                                                                                            
      -- Date information                                                                                                                                   
      f.event_date,                                                                                                                                         
      f.event_datetime,                                                                                                                                     
      d.day_name,                                                                                                                                           
      d.month_name,                                                                                                                                         
      d.year,                                                                                                                                               
      d.quarter,                                                                                                                                            
                                                                                                                                                            
      -- Venue information                                                                                                                                  
      v.venue_id,                                                                                                                                           
      v.venue_name,                                                                                                                                         
      v.city,                                                                                                                                               
      v.state,                                                                                                                                              
      v.country,                                                                                                                                            
      v.latitude,                                                                                                                                           
      v.longitude,                                                                                                                                          
                                                                                                                                                            
      -- Classification information                                                                                                                         
      c.segment_name,                                                                                                                                       
      c.genre_name,                                                                                                                                         
      c.subgenre_name,                                                                                                                                      
      c.type_name,                                                                                                                                          
      c.subtype_name,                                                                                                                                       
                                                                                                                                                            
      -- Pricing                                                                                                                                            
      f.price_min,                                                                                                                                          
      f.price_max,                                                                                                                                          
                                                                                                                                                            
      -- Sales information                                                                                                                                  
      f.sales_start_datetime,                                                                                                                               
      f.sales_end_datetime,                                                                                                                                 
                                                                                                                                                            
      -- Status flag                                                                                                                                        
      f.is_test,                                                                                                                                            
                                                                                                                                                            
      -- Aggregated attraction information (prevents fan-out)                                                                                               
      COALESCE(attr.attraction_count, 0) as attraction_count,                                                                                               
      attr.attraction_names as attraction_names,                                                                                                                                
      attr.attraction_types,                                                                                                                                
                                                                                                                                                            
      -- URLs                                                                                                                                               
      f.event_url                                                                                                                                           
                                                                                                                                                            
  FROM ticket_master.gold.fact_events f                                                                                                                     
                                                                                                                                                            
  -- Join to date dimension (no SCD, direct join)                                                                                                           
  INNER JOIN ticket_master.gold.dim_date d                                                                                                                  
      ON f.date_sk_fk = d.date_sk                                                                                                                           
                                                                                                                                                            
  -- Join to venue dimension (SCD Type 2 - need is_current filter)                                                                                          
  LEFT JOIN ticket_master.gold.dim_venue v                                                                                                                  
      ON f.venue_sk_fk = v.venue_sk                                                                                                                         
      AND v.is_current = TRUE                                                                                                                               
                                                                                                                                                            
  -- Join to classification dimension (SCD Type 2 - need is_current filter)                                                                                 
  LEFT JOIN ticket_master.gold.dim_classification c                                                                                                         
      ON f.classification_sk_fk = c.classification_sk                                                                                                       
      AND c.is_current = TRUE                                                                                                                               
                                                                                                                                                            
  -- Subquery to aggregate attractions per event (prevents fan-out)                                                                                         
  LEFT JOIN (                                                                                                                                               
      SELECT                                                                                                                                                
          ea.event_sk_fk,                                                                                                                                   
          COUNT(DISTINCT ea.attraction_sk_fk) as attraction_count,                                                                                          
          string_agg(DISTINCT a.attraction_name, ', ') as attraction_names,                                                                                    
          string_agg(DISTINCT a.attraction_type, ', ') as attraction_types                                                                                      
      FROM ticket_master.gold.bridge_event_attractions ea                                                                                                   
      INNER JOIN ticket_master.gold.dim_attraction a                                                                                                        
          ON ea.attraction_sk_fk = a.attraction_sk                                                                                                          
          AND a.is_current = TRUE                                                                                                                           
      GROUP BY ea.event_sk_fk                                                                                                                               
  ) attr ON f.event_sk = attr.event_sk_fk                                                                                                                   
                                                                                                                                                            
  WHERE f.is_test = FALSE                                                                                                                                   
  ORDER BY f.event_datetime DESC;