SELECT 
query_text, query_id, 
schema_name, warehouse_name,
 execution_status, start_time, end_time
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE
execution_status = 'RUNNING'
and USER_NAME = 'CARLOS_MERCADO' 
ORDER BY START_TIME DESC;

SELECT SYSTEM$CANCEL_QUERY('01b8fc15-0612-6b79-3d4f-8302bb66e893');
