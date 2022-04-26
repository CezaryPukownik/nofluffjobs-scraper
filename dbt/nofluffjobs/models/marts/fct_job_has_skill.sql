with jobs as (

	SELECT * FROM {{ ref('stg_nofluffjobs') }}

),

final as (
	
	SELECT 
		job_id,
		skills->>'type' as skill_type,
		lower(skills->>'name') as skill_name
	FROM jobs
		CROSS JOIN LATERAL json_array_elements(job_skills :: json) skills

)

SELECT * FROM final