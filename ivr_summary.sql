CREATE OR REPLACE TABLE keepcoding.ivr_summary AS
WITH tabla_principal
AS (SELECT calls_ivr_id AS ivr_id
          ,calls_phone_number AS phone_number
          ,calls_ivr_result AS ivr_result
          ,CASE WHEN STRPOS(calls_vdn_label,'ATC') = 1 THEN 'FRONT'
                WHEN STRPOS(calls_vdn_label,'TECH') = 1 THEN 'TECH'
                WHEN STRPOS(calls_vdn_label,'ABSORPTION') = 1 THEN 'ABSORPTION'
                ELSE 'RESTO'
          END AS vdn_aggregation
          ,calls_start_date AS start_date
          ,calls_end_date AS end_date
          ,calls_total_duration AS total_duration
          ,calls_customer_segment AS customer_segment
          ,calls_ivr_language AS ivr_language
          ,calls_steps_module AS steps_module
          ,calls_module_aggregation AS module_aggregation
 FROM keepcoding.ivr_detail
 QUALIFY ROW_NUMBER() OVER(PARTITION BY calls_ivr_id ORDER BY calls_start_date ASC) = 1)
 , tipo_documento
 AS (SELECT calls_ivr_id AS ivr_id
           ,IF(ivr_detail.document_type <> 'NULL', 1, 2) AS rn
           ,IF(ivr_detail.document_type = 'NULL', 'DESCONOCIDO', ivr_detail.document_type) AS document_type
 FROM keepcoding.ivr_detail
 QUALIFY ROW_NUMBER() OVER(PARTITION BY ivr_id ORDER BY rn ASC) = 1)
 , identificacion_documento
 AS (SELECT calls_ivr_id AS ivr_id
           ,IF(ivr_detail.document_identification <> 'NULL', 1, 2) AS rn
           ,IF(ivr_detail.document_identification = 'NULL', 'DESCONOCIDO', ivr_detail.document_identification) AS document_identification
  FROM keepcoding.ivr_detail
  QUALIFY ROW_NUMBER() OVER(PARTITION BY ivr_id ORDER BY rn ASC) = 1)
  , identificacion_telefono
 AS (SELECT calls_ivr_id AS ivr_id
           ,IF(ivr_detail.customer_phone <> 'NULL', 1, 2) AS rn
           ,IF(ivr_detail.customer_phone = 'NULL', 'DESCONOCIDO', ivr_detail.customer_phone) AS customer_phone
  FROM keepcoding.ivr_detail
  QUALIFY ROW_NUMBER() OVER(PARTITION BY ivr_id ORDER BY rn ASC) = 1)
  , identificacion_cuenta_bancaria
 AS (SELECT calls_ivr_id AS ivr_id
           ,IF(ivr_detail.billing_account_id <> 'NULL', 1, 2) AS rn
           ,IF(ivr_detail.billing_account_id = 'NULL', 'DESCONOCIDO', ivr_detail.billing_account_id) AS billing_account_id
  FROM keepcoding.ivr_detail
  QUALIFY ROW_NUMBER() OVER(PARTITION BY ivr_id ORDER BY rn ASC) = 1)
, flag_masiva_lg
 AS (SELECT calls_ivr_id AS ivr_id
           ,IF(ivr_detail.module_name = 'AVERIA_MASIVA', 1, 0) AS masiva_lg
  FROM keepcoding.ivr_detail
  QUALIFY ROW_NUMBER() OVER(PARTITION BY ivr_id ORDER BY masiva_lg DESC) = 1)
, flag_info_phone
 AS (SELECT calls_ivr_id AS ivr_id
           ,IF(ivr_detail.step_name = 'CUSTOMERINFOBYPHONE.TX' AND ivr_detail.step_description_error = 'NULL', 1, 0) AS info_by_phone_lg
  FROM keepcoding.ivr_detail
  QUALIFY ROW_NUMBER() OVER(PARTITION BY ivr_id ORDER BY info_by_phone_lg DESC) = 1)
  , flag_info_dni
 AS (SELECT calls_ivr_id AS ivr_id
           ,IF(ivr_detail.step_name = 'CUSTOMERINFOBYDNI.TX' AND ivr_detail.step_description_error = 'NULL', 1, 0) AS info_by_dni_lg
  FROM keepcoding.ivr_detail
  QUALIFY ROW_NUMBER() OVER(PARTITION BY ivr_id ORDER BY info_by_dni_lg DESC) = 1)
  , repeated_24
  AS (SELECT tabla1.calls_ivr_id AS ivr_id
      ,MAX(IF(TIMESTAMP_DIFF(tabla1.calls_start_date,tabla2.calls_start_date, MINUTE) < 1440 AND TIMESTAMP_DIFF(tabla1.calls_start_date,tabla2.calls_start_date, MINUTE) >= 0, 1, 0)) AS repeated_phone_24H
      ,MAX(IF(TIMESTAMP_DIFF(tabla1.calls_start_date,tabla2.calls_start_date, MINUTE) > -1440 AND TIMESTAMP_DIFF(tabla1.calls_start_date,tabla2.calls_start_date, MINUTE) <= 0, 1, 0)) AS cause_recall_phone_24H
FROM `keepcoding.ivr_detail` tabla1
LEFT
JOIN `keepcoding.ivr_detail` tabla2
ON tabla1.calls_phone_number = tabla2.calls_phone_number AND tabla1.calls_ivr_id <> tabla2.calls_ivr_id
GROUP BY tabla1.calls_ivr_id)


SELECT tabla_principal.ivr_id
      ,tabla_principal.phone_number
      ,tabla_principal.ivr_result
      ,tabla_principal.vdn_aggregation
      ,tabla_principal.start_date
      ,tabla_principal.end_date
      ,tabla_principal.total_duration
      ,tabla_principal.customer_segment
      ,tabla_principal.ivr_language
      ,tabla_principal.steps_module
      ,tabla_principal.module_aggregation
      ,tipo_documento.document_type
      ,identificacion_documento.document_identification
      ,identificacion_telefono.customer_phone
      ,identificacion_cuenta_bancaria.billing_account_id
      ,flag_masiva_lg.masiva_lg
      ,flag_info_phone.info_by_phone_lg
      ,flag_info_dni.info_by_dni_lg
      ,repeated_24.repeated_phone_24H
      ,repeated_24.cause_recall_phone_24H
 FROM tabla_principal
 LEFT
 JOIN tipo_documento
 ON tabla_principal.ivr_id = tipo_documento.ivr_id
 LEFT
 JOIN identificacion_documento
 ON tabla_principal.ivr_id = identificacion_documento.ivr_id
 LEFT
 JOIN identificacion_telefono
 ON tabla_principal.ivr_id = identificacion_telefono.ivr_id
 LEFT 
 JOIN identificacion_cuenta_bancaria
 ON tabla_principal.ivr_id = identificacion_cuenta_bancaria.ivr_id
 LEFT 
 JOIN flag_masiva_lg
 ON tabla_principal.ivr_id = flag_masiva_lg.ivr_id
 LEFT 
 JOIN flag_info_phone
 ON tabla_principal.ivr_id = flag_info_phone.ivr_id
 LEFT 
 JOIN flag_info_dni
 ON tabla_principal.ivr_id = flag_info_dni.ivr_id
 LEFT
 JOIN repeated_24
 ON tabla_principal.ivr_id = repeated_24.ivr_id