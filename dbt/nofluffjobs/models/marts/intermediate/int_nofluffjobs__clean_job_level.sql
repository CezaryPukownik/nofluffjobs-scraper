with jobs as (

	SELECT * FROM {{ ref('stg_nofluffjobs') }}

),

final as (

	SELECT DISTINCT 
		job_level,  
		CASE 
			WHEN job_level in ('["Trainee"]') THEN 'Trainee'
			WHEN job_level in ('["Junior", "Trainee"]', '["Trainee", "Junior"]') THEN 'Trainee+'
			WHEN job_level in ('["Junior"]') THEN 'Junior'
			WHEN job_level in ('["Junior", "Mid"]', '["Mid", "Junior"]') THEN 'Junior+'
			WHEN job_level in ('["Mid"]') THEN 'Mid'
			WHEN job_level in ('["Mid", "Senior"]', '["Senior", "Mid"]') THEN 'Mid+'
			WHEN job_level in ('["Senior"]') THEN 'Senior'
			WHEN job_level in ('["Senior", "Expert"]', '["Expert", "Senior"]') THEN 'Senior+'
			WHEN job_level in ('["Expert"]') THEN 'Expert'
		END as job_level_name,
			CASE 
			WHEN job_level in ('["Trainee"]') THEN 0
			WHEN job_level in ('["Junior", "Trainee"]', '["Trainee", "Junior"]') THEN 1
			WHEN job_level in ('["Junior"]') THEN 2
			WHEN job_level in ('["Junior", "Mid"]', '["Mid", "Junior"]') THEN 3
			WHEN job_level in ('["Mid"]') THEN 4
			WHEN job_level in ('["Mid", "Senior"]', '["Senior", "Mid"]') THEN 5
			WHEN job_level in ('["Senior"]') THEN 6
			WHEN job_level in ('["Senior", "Expert"]', '["Expert", "Senior"]') THEN 7
			WHEN job_level in ('["Expert"]') THEN 8
		END as job_level_order
	FROM jobs

)

SELECT * FROM final