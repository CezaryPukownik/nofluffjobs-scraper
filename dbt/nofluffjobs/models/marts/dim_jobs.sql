with jobs as (

	SELECT * FROM {{ ref('stg_nofluffjobs') }}
),

salary as (

	SELECT * FROM {{ ref('int_nofluffjobs__salary_pivot') }}
),

job_level as (

	SELECT * FROM {{ ref('int_nofluffjobs__clean_job_level') }}

)

final as (

	SELECT
		jobs.job_id,
		jobs.job_name,
		UPPER(TRIM(
		REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
		REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
		REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(REPLACE(
		REPLACE(REPLACE(REPLACE(jobs.job_name, 'Remote', ''), 'Senior', ''), 'Junior', '')
			, 'Regular', ''), '\(.*\)$', ''), 'Business Intelligence', 'BI'), 'with', ''), 'Python', ''),
		'Power BI', 'BI'), '/', ''), '-', ''), 'PowerBI', ''), 'Machine Learning', 'ML'),
		'Big', ''), 'Scala', ''), '(', ''), ')', ''), 'AI', 'ML')
		)) as job_type,
		jobs.job_location,
		jobs.company_name,
		COALESCE(salary.uop_salary, salary.b2b_salary, salary.uz_salary) as salary,
		job_level.job_level_name,
		job_level.job_level_order,
		jobs._url


	FROM jobs
		LEFT JOIN salary on salary.job_id = jobs.job_id
		LEFT JOIN job_level on job_level.job_level = jobs.job_level

)

SELECT * FROM final