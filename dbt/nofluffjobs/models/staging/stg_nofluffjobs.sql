with source as (

	{{ source('nofluffjobs', 'nofluffjobs') }}

),

final as (

	SELECT
		_id as job_id,
		TRIM(company) company_name,
		TRIM(title) job_name,
		job_level,
		TRIM(city) job_location,
		locations job_location_details,
		salary job_salary_details,
		category job_categories,
		skills job_skills,
		CASE WHEN city = 'Remote' THEN 1 ELSE 0 END is_remote,
		timestamp measured_at,
		url _url

	FROM source

)

SELECT * FROM final

-- job_has_level
-- job_has_location
-- job_has_salary
-- job_has_category
-- job_has_skill