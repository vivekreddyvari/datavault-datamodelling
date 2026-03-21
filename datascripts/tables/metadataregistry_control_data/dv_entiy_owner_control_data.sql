INSERT INTO dev.dev_metadataregistry.dv_entity_owner (
    entity_name,
    owner_dataset,
    hub_name,
    bk_columns,
    satellite_name,
    satellite_mode,
    attr_allowlist,
    attr_denylist,
    is_active,
    created_at
) VALUES
    ('Customer', 'customers.csv', 'H_CUSTOMER', '["CustomerID"]', 'S_CUSTOMER', 'ALLOWLIST',
     '["CompanyName", "ContactName", "ContactTitle", "Address", "City", "Region", "PostalCode", "Country", "Phone", "Fax"]',
     '[]', TRUE, CURRENT_TIMESTAMP()),
    
    ('Order', 'orders.csv', 'H_ORDER', '["OrderID"]', 'S_ORDER', 'ALLOWLIST',
     '["CustomerID", "EmployeeID", "OrderDate", "RequiredDate", "ShippedDate", "ShipVia", "Freight", "ShipName", "ShipAddress", "ShipCity", "ShipRegion", "ShipPostalCode", "ShipCountry"]',
     '[]', TRUE, CURRENT_TIMESTAMP()),
    
    ('Product', 'products.csv', 'H_PRODUCT', '["ProductID"]', 'S_PRODUCT', 'ALLOWLIST',
     '["ProductName", "SupplierID", "CategoryID", "QuantityPerUnit", "UnitPrice", "UnitsInStock", "UnitsOnOrder", "ReorderLevel", "Discontinued"]',
     '[]', TRUE, CURRENT_TIMESTAMP()),
    
    ('OrderDetail', 'order-details.csv', 'H_ORDER_DETAIL', '["OrderID", "ProductID"]', 'S_ORDER_DETAIL', 'ALLOWLIST',
     '["UnitPrice", "Quantity", "Discount"]',
     '[]', TRUE, CURRENT_TIMESTAMP()),
    
    ('Employee', 'employees.csv', 'H_EMPLOYEE', '["EmployeeID"]', 'S_EMPLOYEE', 'ALLOWLIST',
     '["LastName", "FirstName", "Title", "TitleOfCourtesy", "BirthDate", "HireDate", "Address", "City", "Region", "PostalCode", "Country", "HomePhone", "Extension", "Notes", "ReportsTo"]',
     '[]', TRUE, CURRENT_TIMESTAMP()),
    
    ('Shipper', 'shippers.csv', 'H_SHIPPER', '["ShipperID"]', 'S_SHIPPER', 'ALLOWLIST',
     '["CompanyName", "Phone"]',
     '[]', TRUE, CURRENT_TIMESTAMP()),
    
    ('Category', 'categories.csv', 'H_CATEGORY', '["categoryID"]', 'S_CATEGORY', 'ALLOWLIST',
     '["categoryName", "description", "picture"]',
     '[]', TRUE, CURRENT_TIMESTAMP()),
    
    ('Territory', 'territories.csv', 'H_TERRITORY', '["TerritoryID"]', 'S_TERRITORY', 'ALLOWLIST',
     '["TerritoryDescription", "RegionID"]',
     '[]', TRUE, CURRENT_TIMESTAMP()),
    
    ('Region', 'regions.csv', 'H_REGION', '["RegionID"]', 'S_REGION', 'ALLOWLIST',
     '["RegionDescription"]',
     '[]', TRUE, CURRENT_TIMESTAMP()),
    
    ('Supplier', 'products.csv', 'H_SUPPLIER', '["SupplierID"]', 'S_SUPPLIER', 'ALLOWLIST',
     '["CompanyName", "ContactName", "ContactTitle", "Address", "City", "Region", "PostalCode", "Country", "Phone", "Fax"]',
     '[]', TRUE, CURRENT_TIMESTAMP());


