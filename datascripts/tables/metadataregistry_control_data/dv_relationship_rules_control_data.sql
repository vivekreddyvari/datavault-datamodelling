-- Relationship Rules Control Data for NorthWind Data Vault
-- Based on: Data Vault.xlsx - NorthWind-Natural Business Relationships sheet
-- These rules define constraints on which hub pairs can participate in links

INSERT INTO dev.dev_metadataregistry.dv_relationship_rules (
    rule_set,
    allow_pairs,
    deny_pairs,
    max_hubs_per_link,
    require_nbr_for_links,
    created_at
) VALUES
    (
        'northwind_rules',
        -- Allowed hub pairs based on NBR relationships documented in Data Vault.xlsx
        'Customer-Order;Order-OrderDetail;OrderDetail-Product;Product-Supplier;Product-Category;Employee-Order;Order-Shipper;Employee-Territory;Territory-Region',
        -- Denied pairs (relationships explicitly NOT allowed)
        'Customer-Product;Customer-Employee;Customer-Supplier;Customer-Shipper;OrderDetail-Supplier;OrderDetail-Shipper;Supplier-Shipper;Employee-Product',
        -- Maximum number of hubs that can participate in a single link
        4,
        -- Require explicit NBR entry for creating links
        TRUE,
        CURRENT_TIMESTAMP()
    );
