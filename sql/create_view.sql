CREATE VIEW IF NOT EXISTS working_data_view AS
SELECT
    JSONExtractInt(dado_linha, 'id_employee') AS id_employee,
    JSONExtract(dado_linha, 'store_id', 'String') AS store_id,
	JSONExtract(dado_linha, 'status', 'String') AS status
FROM working_data;