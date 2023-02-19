CREATE OR REPLACE FUNCTION keepcoding.clean_integer (p_string STRING) RETURNS STRING
AS ((SELECT IF(UPPER(p_string)='NULL' OR p_string IS NULL,'-999999',p_string)));