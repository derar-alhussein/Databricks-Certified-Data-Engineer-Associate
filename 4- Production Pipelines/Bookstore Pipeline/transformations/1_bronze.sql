CREATE OR REFRESH STREAMING TABLE orders_raw
COMMENT "The raw books orders, ingested from orders-raw"
AS SELECT * FROM STREAM read_files("${datasets_path}/orders-json-raw", 
                                    format => 'json',
                                    inferColumnTypes => true);

CREATE OR REFRESH MATERIALIZED VIEW customers
COMMENT "The customers lookup table, ingested from customers-json"
AS SELECT * FROM json.`${datasets_path}/customers-json`