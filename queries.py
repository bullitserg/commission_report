get_procedure_request_info_query = '''SELECT
  p.registrationNumber AS registration_number,
  IFNULL(lc.provisionAmount, 0) AS provision_amount,
  REPLACE(o.fullName, "'", '"') AS supplier_full_name,
  REPLACE(o.shortName, "'", '"') AS supplier_short_name,
  o.inn AS supplier_inn,
  o.kpp AS supplier_kpp,
  IF(pf.id IS NOT NULL, 'Да', 'Нет') AS is_smp,
  c.actualCustomerSignToDateTime AS contract_datetime,
  l.maxSum AS max_sum,
  a.customAddress AS supplier_address,
  r.additionalData AS additional_data,
  r.id AS request_id
FROM procedures p
  JOIN procedureLot l
    ON l.procedureId = p.id
    AND l.actualId IS NULL
    AND l.archive = 0
  JOIN procedureLotCustomer lc
    ON lc.lotId = l.id
    AND lc.actualId IS NULL
    AND lc.archive = 0
  JOIN procedureContract c
    ON c.procedureId = p.id
    AND c.customerId = lc.organizationId
    AND c.actualId IS NULL
    AND c.active = 1
    AND c.contractStatusId = 15
    AND c.actualCustomerSignToDateTime BETWEEN DATE_FORMAT(SUBDATE(NOW(), INTERVAL 1 DAY), '%Y-%m-%d 00:00:00')
  AND DATE_FORMAT(SUBDATE(NOW(), INTERVAL 1 DAY), '%Y-%m-%d 23:59:59')
JOIN

(
SELECT
  c.procedureId,
  MIN(c.actualCustomerSignToDateTime) AS first_contract_date
  FROM procedureContract c
   WHERE c.actualId IS NULL
    AND c.active = 1
    AND c.contractStatusId = 15
  GROUP BY c.procedureId
  ) AS first_contract_tb
  ON first_contract_tb.procedureId = p.id
  AND first_contract_tb.first_contract_date = c.actualCustomerSignToDateTime

  JOIN organization o
    ON o.id = c.supplierId
  JOIN procedureRequest r
    ON r.procedureId = p.id
    AND r.organizationId = o.id
    AND r.actualId IS NULL
    AND r.active = 1
    AND r.requestStatusId = 20
  JOIN organizationAddress a
    ON a.id = o.factualAddressId
  LEFT JOIN procedureRestriction pf ON pf.procedureId = p.id
    AND pf.preferenceTypeId = 45

WHERE p.publicationDateTime > '2018-10-01 00:00:00'
AND p.actualId IS NULL
AND p.archive = 0
AND p.procedureStatusId != 80
AND p.procedureTypeId = 5
ORDER BY contract_datetime DESC
;'''

get_edo_info_query = '''SELECT
  b.code AS bank_code,
  a.account_number
FROM edo.account a
  LEFT JOIN edo.bank b
    ON b.id = a.bank_id
WHERE a.id = '%s';'''


get_commission_status_info_query = '''SELECT
  GROUP_CONCAT(p.amount SEPARATOR '\n') AS real_amount,
  GROUP_CONCAT(p.status SEPARATOR '\n') AS real_status,
  GROUP_CONCAT(CASE p.status
  WHEN -30 THEN 'Проблемное списание комиссии'
  WHEN -10 THEN 'Проблемное списание комиссии'
  WHEN 0 THEN 'Статус списания комиссии неизвестен'
  WHEN 1 THEN 'Комиссия списана'
  WHEN 2 THEN 'Списание комиссии завершено ошибкой'
  END SEPARATOR '\n') AS real_operation_status_text,
  GROUP_CONCAT(p.description SEPARATOR '\n') AS real_description
FROM payment p
WHERE p.purchase_number = '%(registration_number)s'
AND p.inn = '%(supplier_inn)s'
AND p.type = 'commission'
;'''


# get_good_commission_info_query = '''SELECT
#   p1.registrationNumber AS 'Номер закупки',
#   p.publicationDateTime AS 'Дата публикации протокола',
#   o.inn AS 'Участник для списания комиссии'
# FROM procedureProtocol p
#   JOIN procedureProtocolContractRefuse pcr
#     ON pcr.id = p.id
#     AND pcr.initiator = 'customer'
#     AND pcr.refuseStatusId = 54
#   JOIN procedureContract c
#     ON c.id = pcr.contractId
#     AND c.actualId IS NULL
#   JOIN organization o
#     ON o.id = c.supplierId
#   JOIN procedures p1
#     ON p1.id = p.procedureId
#     AND p1.publicationDateTime > '2018-10-01 00:00:00'
# WHERE p.typeCode IN ('protocol.contract.refuse')
# AND p.status = 24
# AND p.actualId IS NULL
# AND p.publicationDateTime BETWEEN DATE_FORMAT(SUBDATE(NOW(), INTERVAL 1 DAY), '%Y-%m-%d 00:00:00')
#   AND DATE_FORMAT(SUBDATE(NOW(), INTERVAL 1 DAY), '%Y-%m-%d 23:59:59')
# GROUP BY p1.registrationNumber
# HAVING COUNT(p.id) = 1
# ;'''


get_good_commission_info_query = '''SELECT
  p.registrationNumber AS registration_number,
  SUM(IFNULL(lc.provisionAmount, 0)) AS provision_amount,
  REPLACE(o.fullName, "'", '"') AS supplier_full_name,
  REPLACE(o.shortName, "'", '"') AS supplier_short_name,
  o.inn AS supplier_inn,
  o.kpp AS supplier_kpp,
  IF(pf.id IS NOT NULL, 'Да', 'Нет') AS is_smp,
  protocol_table.publicationDateTime AS contract_datetime,
  l.maxSum AS max_sum,
  a.customAddress AS supplier_address,
  r.additionalData AS additional_data,
  r.id AS request_id
FROM procedures p
  JOIN procedureLot l
    ON l.procedureId = p.id
    AND l.actualId IS NULL
    AND l.archive = 0
  JOIN procedureLotCustomer lc
    ON lc.lotId = l.id
    AND lc.actualId IS NULL
    AND lc.archive = 0
  JOIN procedureRequest r
    ON r.procedureId = p.id
    AND r.actualId IS NULL
    AND r.active = 1
    AND r.requestStatusId = 20
  JOIN organization o
    ON o.id = r.organizationId
  JOIN
  (SELECT
  p1.id AS procedure_id,
  p.publicationDateTime,
  o.id AS supplier_id
FROM procedureProtocol p
  JOIN procedureProtocolContractRefuse pcr
    ON pcr.id = p.id
    AND pcr.initiator = 'customer'
    AND pcr.refuseStatusId = 54
  JOIN procedureContract c
    ON c.id = pcr.contractId
    AND c.actualId IS NULL
  JOIN organization o
    ON o.id = c.supplierId
  JOIN procedures p1
    ON p1.id = p.procedureId
    AND p1.publicationDateTime > '2018-10-01 00:00:00'
WHERE p.typeCode IN ('protocol.contract.refuse')
AND p.status = 24
AND p.actualId IS NULL
AND p.publicationDateTime BETWEEN DATE_FORMAT(SUBDATE(NOW(), INTERVAL 1 DAY), '%Y-%m-%d 00:00:00')
  AND DATE_FORMAT(SUBDATE(NOW(), INTERVAL 1 DAY), '%Y-%m-%d 23:59:59')
GROUP BY p1.registrationNumber
HAVING COUNT(p.id) = 1) AS protocol_table
  ON protocol_table.procedure_id = p.id
  AND protocol_table.supplier_id = o.id
  JOIN organizationAddress a
    ON a.id = o.factualAddressId
  LEFT JOIN procedureRestriction pf ON pf.procedureId = p.id
    AND pf.preferenceTypeId = 45

WHERE p.publicationDateTime > '2018-10-01 00:00:00'
AND p.actualId IS NULL
AND p.archive = 0
AND p.procedureStatusId != 80
AND p.procedureTypeId = 5
ORDER BY protocol_table.publicationDateTime DESC
;'''

get_error_commission_info_query = '''SELECT
  p1.registrationNumber AS 'Номер закупки',
  p.publicationDateTime AS 'Дата публикации протокола',
  o1.inn AS 'Участник с которого некорректно списана комиссия',
  c1.actualCustomerSignToDateTime AS 'Дата подписания контракта',
  o.inn AS 'Участник с которого надлежит списать комиссию'
FROM procedureProtocol p
  JOIN procedureProtocolContractRefuse pcr
    ON pcr.id = p.id
    AND pcr.initiator = 'customer'
    AND pcr.refuseStatusId = 54
  JOIN procedureContract c
    ON c.id = pcr.contractId
    AND c.actualId IS NULL
  JOIN organization o
    ON o.id = c.supplierId
  JOIN procedureContract c1
    ON c1.procedureId = p.procedureId
    AND c1.supplierId != c.supplierId
    AND c1.contractStatusId = 15
    AND c1.actualId IS NULL
    AND c1.actualCustomerSignToDateTime BETWEEN DATE_FORMAT(SUBDATE(NOW(), INTERVAL 1 DAY), '%Y-%m-%d 00:00:00')
  AND DATE_FORMAT(SUBDATE(NOW(), INTERVAL 1 DAY), '%Y-%m-%d 23:59:59')
  JOIN organization o1
    ON o1.id = c1.supplierId
  JOIN procedures p1
    ON p1.id = p.procedureId
    AND p1.publicationDateTime > '2018-10-01 00:00:00'
WHERE p.typeCode IN ('protocol.contract.refuse')
AND p.status = 24
AND p.actualId IS NULL
GROUP BY p1.registrationNumber
HAVING COUNT(p.id) = 1
ORDER BY c1.actualCustomerSignToDateTime DESC
;'''