-- Replace `your_project_id` and `your_dataset` with your actual Google Cloud project and dataset names.

DROP VIEW IF EXISTS `your_project_id.your_dataset.latest_update_view`;

CREATE VIEW `your_project_id.your_dataset.latest_update_view` AS
SELECT 'your_table_name' AS table_name, MAX(date) AS latest_date
FROM `your_project_id.your_dataset.your_table_name`;
