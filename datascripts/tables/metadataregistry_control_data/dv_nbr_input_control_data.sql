INSERT INTO dev.dev_metadataregistry_control_data.dv_nbr_input (
  relationship_id, 
  relationship_name,
  left_entity,  
  right_entity, 
  source_dataset, 
  date_of_entry,
  business_key_map,
  is_active, 
  created_at
) VALUES 
    ('REL-001', 'Customer_places_Order', 'Customer', 'Order', 'orders.csv', '2026-03-20',
     '{"Customer": ["customer_id"], "Order": ["order_id"]}', TRUE, CURRENT_TIMESTAMP()),
    
    ('REL-002', 'Order_contains_OrderDetail', 'Order', 'OrderDetail', 'order_details.csv', '2026-03-20',
     '{"Order": ["order_id"], "OrderDetail": ["order_id", "product_id"]}', TRUE, CURRENT_TIMESTAMP()),
    
    ('REL-003', 'Product_supplied_by_Supplier', 'Product', 'Supplier', 'products.csv', '2026-03-20',
     '{"Product": ["product_id"], "Supplier": ["supplier_id"]}', TRUE, CURRENT_TIMESTAMP()),
    
    ('REL-004', 'Order_shipped_by_Shipper', 'Order', 'Shipper', 'orders.csv', '2026-03-20',
     '{"Order": ["order_id"], "Shipper": ["shipper_id"]}', TRUE, CURRENT_TIMESTAMP()),
    
    ('REL-005', 'Product_in_Category', 'Product', 'Category', 'products.csv', '2026-03-20',
     '{"Product": ["product_id"], "Category": ["category_id"]}', TRUE, CURRENT_TIMESTAMP()),
    
    ('REL-006', 'Employee_reports_to_Employee', 'Employee', 'Employee', 'employees.csv', '2026-03-20',
     '{"Employee": ["employee_id"], "ReportsTo": ["employee_id"]}', TRUE, CURRENT_TIMESTAMP()),
    
    ('REL-007', 'Employee_in_Territory', 'Employee', 'Territory', 'territory.csv', '2026-03-20',
     '{"Employee": ["employee_id"], "Territory": ["territory_id"]}', TRUE, CURRENT_TIMESTAMP()),
    
    ('REL-008', 'Territory_in_Region', 'Territory', 'Region', 'region.csv', '2026-03-20',
     '{"Territory": ["territory_id"], "Region": ["region_id"]}', TRUE, CURRENT_TIMESTAMP());
