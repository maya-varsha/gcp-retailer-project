-- ============================================================
-- SILVER LAYER - CUSTOMERS
-- ============================================================

-- Step 1: Create the customers Table in the Silver Layer
CREATE TABLE IF NOT EXISTS `project-bd10f83d-812d-48fb-93c.silver_dataset_maya.customers`
(
    customer_id INT64,
    name STRING,
    email STRING,
    updated_at STRING,
    is_quarantined BOOL,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_active BOOL
);


-- Step 2: Update Existing Active Records if There Are Changes
MERGE INTO `project-bd10f83d-812d-48fb-93c.silver_dataset_maya.customers` AS target
USING
(
    SELECT DISTINCT
        customer_id,
        name,
        email,
        updated_at,

        CASE
        #If any of these fields are NULL, is_quarantined = TRUE
            WHEN customer_id IS NULL
              OR email IS NULL
              OR name IS NULL
            THEN TRUE
            ELSE FALSE
        END AS is_quarantined,

        CURRENT_TIMESTAMP() AS effective_start_date,
        #In a typical SCD-2 design, a newly active record would usually have:
        #effective_end_date = NULL
        #until it becomes old.
        CURRENT_TIMESTAMP() AS effective_end_date,
        TRUE AS is_active

    FROM `project-bd10f83d-812d-48fb-93c.bronze_dataset_maya.customers`
) AS source

ON target.customer_id = source.customer_id
AND target.is_active = TRUE

WHEN MATCHED AND
(
    target.name IS DISTINCT FROM source.name
    OR target.email IS DISTINCT FROM source.email
    #Both target and source updated_at are STRING in this dataset,
    #so compare directly without casting.
    OR target.updated_at IS DISTINCT FROM source.updated_at
)

THEN UPDATE SET
    target.is_active = FALSE,
    target.effective_end_date = CURRENT_TIMESTAMP();


-- Step 3: Insert New or Updated Records
MERGE INTO `project-bd10f83d-812d-48fb-93c.silver_dataset_maya.customers` AS target
USING
(
    SELECT DISTINCT
        customer_id,
        name,
        email,
        CAST(updated_at AS STRING) AS updated_at,

        CASE
            WHEN customer_id IS NULL
              OR email IS NULL
              OR name IS NULL
            THEN TRUE
            ELSE FALSE
        END AS is_quarantined,

        CURRENT_TIMESTAMP() AS effective_start_date,
        CURRENT_TIMESTAMP() AS effective_end_date,
        TRUE AS is_active

    FROM `project-bd10f83d-812d-48fb-93c.bronze_dataset_maya.customers`
) AS source

ON target.customer_id = source.customer_id
AND target.is_active = TRUE

WHEN NOT MATCHED THEN
INSERT
(
    customer_id,
    name,
    email,
    updated_at,
    is_quarantined,
    effective_start_date,
    effective_end_date,
    is_active
)
VALUES
(
    source.customer_id,
    source.name,
    source.email,
    source.updated_at,
    source.is_quarantined,
    source.effective_start_date,
    source.effective_end_date,
    source.is_active
);


-- ============================================================
-- SILVER LAYER - ORDERS
-- ============================================================

-- Step 1: Create the orders Table in the Silver Layer
CREATE TABLE IF NOT EXISTS `project-bd10f83d-812d-48fb-93c.silver_dataset_maya.orders`
(
    order_id INT64,
    customer_id INT64,
    order_date STRING,
    total_amount FLOAT64,
    updated_at STRING,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_active BOOL
);


-- ============================================================
-- Step 2: Update Existing Active Records if There Are Changes
-- ============================================================

MERGE INTO `project-bd10f83d-812d-48fb-93c.silver_dataset_maya.orders` AS target

USING
(
    SELECT DISTINCT
        order_id,
        customer_id,

        -- Bronze order_date is TIMESTAMP
        order_date,

        total_amount,

        -- Bronze updated_at is TIMESTAMP
        updated_at,

        CURRENT_TIMESTAMP() AS effective_start_date,
        CURRENT_TIMESTAMP() AS effective_end_date,
        TRUE AS is_active

    FROM `project-bd10f83d-812d-48fb-93c.bronze_dataset_maya.orders`
) AS source

ON target.order_id = source.order_id
AND target.is_active = TRUE

WHEN MATCHED AND
(
    #compare old name vs new name
    target.customer_id IS DISTINCT FROM source.customer_id

    -- Convert Silver STRING to TIMESTAMP before comparison
    OR SAFE_CAST(target.order_date AS TIMESTAMP)
       IS DISTINCT FROM source.order_date

    OR target.total_amount IS DISTINCT FROM source.total_amount

    -- Convert Silver STRING to TIMESTAMP before comparison
    OR SAFE_CAST(target.updated_at AS TIMESTAMP)
       IS DISTINCT FROM source.updated_at
)

THEN UPDATE SET
    target.is_active = FALSE,
    target.effective_end_date = CURRENT_TIMESTAMP();


-- ============================================================
-- Step 3: Insert New or Updated Records
-- ============================================================

MERGE INTO `project-bd10f83d-812d-48fb-93c.silver_dataset_maya.orders` AS target

USING
(
    SELECT DISTINCT
        order_id,
        customer_id,

        -- Convert TIMESTAMP to STRING for Silver table
        CAST(order_date AS STRING) AS order_date,

        total_amount,

        -- Convert TIMESTAMP to STRING for Silver table
        CAST(updated_at AS STRING) AS updated_at,

        CURRENT_TIMESTAMP() AS effective_start_date,
        CURRENT_TIMESTAMP() AS effective_end_date,
        TRUE AS is_active

    FROM `project-bd10f83d-812d-48fb-93c.bronze_dataset_maya.orders`
) AS source

ON target.order_id = source.order_id
AND target.is_active = TRUE

WHEN NOT MATCHED THEN

INSERT
(
    order_id,
    customer_id,
    order_date,
    total_amount,
    updated_at,
    effective_start_date,
    effective_end_date,
    is_active
)

VALUES
(
    source.order_id,
    source.customer_id,
    source.order_date,
    source.total_amount,
    source.updated_at,
    source.effective_start_date,
    source.effective_end_date,
    source.is_active
);

-- ============================================================
-- SILVER LAYER - ORDER ITEMS
-- ============================================================

-- Step 1: Create the order_items Table in the Silver Layer
CREATE TABLE IF NOT EXISTS `project-bd10f83d-812d-48fb-93c.silver_dataset_maya.order_items`
(
    order_item_id INT64,
    order_id INT64,
    product_id INT64,
    quantity INT64,
    price FLOAT64,
    updated_at STRING,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_active BOOL
);


-- Step 2: Update Existing Active Records if There Are Changes
MERGE INTO `project-bd10f83d-812d-48fb-93c.silver_dataset_maya.order_items` AS target
USING
(
    SELECT DISTINCT
        order_item_id,
        order_id,
        product_id,
        quantity,
        price,
        updated_at,

        CURRENT_TIMESTAMP() AS effective_start_date,
        CURRENT_TIMESTAMP() AS effective_end_date,
        TRUE AS is_active

    FROM `project-bd10f83d-812d-48fb-93c.bronze_dataset_maya.order_items`
) AS source

ON target.order_item_id = source.order_item_id
AND target.is_active = TRUE

WHEN MATCHED AND
(
    target.order_id IS DISTINCT FROM source.order_id
    OR target.product_id IS DISTINCT FROM source.product_id
    OR target.quantity IS DISTINCT FROM source.quantity
    OR target.price IS DISTINCT FROM source.price
    OR SAFE_CAST(target.updated_at AS TIMESTAMP)
       IS DISTINCT FROM source.updated_at
)

THEN UPDATE SET
    target.is_active = FALSE,
    target.effective_end_date = CURRENT_TIMESTAMP();


-- Step 3: Insert New or Updated Records
MERGE INTO `project-bd10f83d-812d-48fb-93c.silver_dataset_maya.order_items` AS target
USING
(
    SELECT DISTINCT
        order_item_id,
        order_id,
        product_id,
        quantity,
        price,
        CAST(updated_at AS STRING) AS updated_at,

        CURRENT_TIMESTAMP() AS effective_start_date,
        CURRENT_TIMESTAMP() AS effective_end_date,
        TRUE AS is_active

    FROM `project-bd10f83d-812d-48fb-93c.bronze_dataset_maya.order_items`
) AS source

ON target.order_item_id = source.order_item_id
AND target.is_active = TRUE

WHEN NOT MATCHED THEN
INSERT
(
    order_item_id,
    order_id,
    product_id,
    quantity,
    price,
    updated_at,
    effective_start_date,
    effective_end_date,
    is_active
)
VALUES
(
    source.order_item_id,
    source.order_id,
    source.product_id,
    source.quantity,
    source.price,
    source.updated_at,
    source.effective_start_date,
    source.effective_end_date,
    source.is_active
);


-- ============================================================
-- SILVER LAYER - CATEGORIES
-- ============================================================

-- Step 1: Create the categories Table in the Silver Layer
CREATE TABLE IF NOT EXISTS `project-bd10f83d-812d-48fb-93c.silver_dataset_maya.categories`
(
    category_id INT64,
    name STRING,
    updated_at STRING,
    is_quarantined BOOL
);


-- Step 2: Truncate Table
TRUNCATE TABLE
`project-bd10f83d-812d-48fb-93c.silver_dataset_maya.categories`;


-- Step 3: Insert New or Updated Records
INSERT INTO
`project-bd10f83d-812d-48fb-93c.silver_dataset_maya.categories`
(
    category_id,
    name,
    updated_at,
    is_quarantined
)
SELECT
    category_id,
    name,
    CAST(updated_at AS STRING) AS updated_at,

    CASE
        WHEN category_id IS NULL
          OR name IS NULL
        THEN TRUE
        ELSE FALSE
    END AS is_quarantined

FROM
`project-bd10f83d-812d-48fb-93c.bronze_dataset_maya.categories`;


-- ============================================================
-- SILVER LAYER - PRODUCTS
-- ============================================================

-- Step 1: Create the products Table in the Silver Layer
CREATE TABLE IF NOT EXISTS `project-bd10f83d-812d-48fb-93c.silver_dataset_maya.products`
(
    product_id INT64,
    name STRING,
    category_id INT64,
    price FLOAT64,
    updated_at STRING,
    is_quarantined BOOL
);


-- Step 2: Truncate Table
TRUNCATE TABLE
`project-bd10f83d-812d-48fb-93c.silver_dataset_maya.products`;


-- Step 3: Insert New or Updated Records
INSERT INTO
`project-bd10f83d-812d-48fb-93c.silver_dataset_maya.products`
(
    product_id,
    name,
    category_id,
    price,
    updated_at,
    is_quarantined
)
SELECT
    product_id,
    name,
    category_id,
    price,
    CAST(updated_at AS STRING) AS updated_at,

    CASE
        WHEN category_id IS NULL
          OR name IS NULL
        THEN TRUE
        ELSE FALSE
    END AS is_quarantined

FROM
`project-bd10f83d-812d-48fb-93c.bronze_dataset_maya.products`;
-------------------------------------------------------------------------------------------------------------
--Step 1: Create the product_supplier Table in the Silver Layer
CREATE TABLE IF NOT EXISTS `project-bd10f83d-812d-48fb-93c.silver_dataset_maya.product_suppliers`
(
    supplier_id INT64,
    product_id INT64,
    supply_price FLOAT64,
    last_updated STRING,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_active BOOL
);

--Step 2: Update Existing Active Records if There Are Changes
MERGE INTO `project-bd10f83d-812d-48fb-93c.silver_dataset_maya.product_suppliers` target
USING 
  (SELECT 
    *, 
    CURRENT_TIMESTAMP() AS effective_start_date,
    CURRENT_TIMESTAMP() AS effective_end_date,
    TRUE AS is_active
  FROM `project-bd10f83d-812d-48fb-93c.bronze_dataset_maya.product_suppliers`) source
ON target.supplier_id = source.supplier_id 
   AND target.product_id = source.product_id 
   AND target.is_active = true
WHEN MATCHED AND 
            (
             target.supply_price != source.supply_price OR
             target.last_updated != source.last_updated
            ) 
    THEN UPDATE SET 
        target.is_active = false,
        target.effective_end_date = current_timestamp();

--Step 3: Insert New or Updated Records
MERGE INTO `project-bd10f83d-812d-48fb-93c.silver_dataset_maya.product_suppliers` target
USING 
  (SELECT 
    *, 
    CURRENT_TIMESTAMP() AS effective_start_date,
    CURRENT_TIMESTAMP() AS effective_end_date,
    TRUE AS is_active
  FROM `project-bd10f83d-812d-48fb-93c.bronze_dataset_maya.product_suppliers`) source
ON target.supplier_id = source.supplier_id 
   AND target.product_id = source.product_id 
   AND target.is_active = true
WHEN NOT MATCHED THEN 
    INSERT (supplier_id, product_id, supply_price, last_updated, effective_start_date, effective_end_date, is_active)
    VALUES (source.supplier_id, source.product_id, source.supply_price, source.last_updated, source.effective_start_date, source.effective_end_date, source.is_active);


--Step 1: Create the suppliers Table in the Silver Layer
CREATE TABLE IF NOT EXISTS `project-bd10f83d-812d-48fb-93c.silver_dataset_maya.suppliers`
(
  supplier_id INT64,
  supplier_name STRING,
  contact_name STRING,
  phone STRING,
  email STRING,
  address STRING,
  city STRING,
  country STRING,
  created_at STRING,
  is_quarantined BOOL
);

--Step 2: Truncate table
TRUNCATE TABLE `project-bd10f83d-812d-48fb-93c.silver_dataset_maya.suppliers`;

--Step 3: Insert New or Updated Records
INSERT INTO `project-bd10f83d-812d-48fb-93c.silver_dataset_maya.suppliers`
SELECT 
  *,
  CASE 
    WHEN supplier_id IS NULL OR supplier_name IS NULL THEN TRUE
    ELSE FALSE
  END AS is_quarantined
  
FROM `project-bd10f83d-812d-48fb-93c.bronze_dataset_maya.suppliers`;

-------------------------------------------------------------------------------------------------------------

--Step 1: Create the customer_reviews Table in the Silver Layer
CREATE TABLE IF NOT EXISTS `project-bd10f83d-812d-48fb-93c.silver_dataset_maya.customer_reviews`
(
    id STRING,
    customer_id INT64,
    product_id INT64,
    rating INT64,
    review_text STRING,
    review_date STRING,
    # tells us when this version of the review became active.
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    # tells us if the record is currently active or has been superseded by a newer version
    is_active BOOL
);

--Step 2: Update Existing Active Records if There Are Changes
MERGE INTO `project-bd10f83d-812d-48fb-93c.silver_dataset_maya.customer_reviews` target
USING 
  (SELECT 
    *, 
    CURRENT_TIMESTAMP() AS effective_start_date,
    #in a typical SCD-2 design, the current record's effective_end_date is usually NULL until it becomes inactive.
    CURRENT_TIMESTAMP() AS effective_end_date,
    TRUE AS is_active
  FROM `project-bd10f83d-812d-48fb-93c.bronze_dataset_maya.customer_reviews`) source
ON target.id = source.id AND target.is_active = true
WHEN MATCHED AND 
            (
             target.customer_id != source.customer_id OR
             target.product_id != source.product_id OR
             target.rating != source.rating OR
             target.review_text != source.review_text OR
             target.review_date != source.review_date
            ) 
    THEN UPDATE SET 
        target.is_active = false,
        target.effective_end_date = current_timestamp();

--Step 3: Insert New or Updated Records
MERGE INTO `project-bd10f83d-812d-48fb-93c.silver_dataset_maya.customer_reviews` target
USING 
  (SELECT 
    *, 
    CURRENT_TIMESTAMP() AS effective_start_date,
    CURRENT_TIMESTAMP() AS effective_end_date,
    TRUE AS is_active
  FROM `project-bd10f83d-812d-48fb-93c.bronze_dataset_maya.customer_reviews`) source
ON target.id = source.id AND target.is_active = true
WHEN NOT MATCHED THEN 
    INSERT (id, customer_id, product_id, rating, review_text, review_date, effective_start_date, effective_end_date, is_active)
    VALUES (source.id, source.customer_id, source.product_id, source.rating, source.review_text, source.review_date, source.effective_start_date, source.effective_end_date, source.is_active);
-------------------------------------------------------------------------------------------------------------