with jobs as (

	SELECT * FROM {{ ref('stg_nofluffjobs') }}

),

unpacked as (

	SELECT 
		job_id,
		salary->>'type' as salary_type,
		CAST(salary->>'min' AS decimal) as salary_lower,
		cast(salary->>'max' as decimal)as salary_upper
	FROM jobs
		CROSS JOIN LATERAL json_array_elements(job_salary_details :: json) salary

),

better_contracts as (

	SELECT 
		job_id,
		CASE 
			WHEN salary_type like '%B2B%' THEN 'B2B'
			WHEN salary_type like '%UoP%' THEN 'UoP'
			WHEN salary_type like '%UZ%' THEN 'UZ'
			ELSE NULL
		END as contract_type,
		CASE 
			WHEN salary_type like '%dziennie%' THEN 'D'
			WHEN salary_type like '%miesięcznie%' THEN 'M'
			WHEN salary_type like '%godzinowo%' THEN 'H'
			WHEN salary_type like '%rocznie%' THEN 'A'
			ELSE NULL
		END as salary_period,
		COALESCE(salary_lower, salary_upper) as salary_lower,
		COALESCE(salary_upper, salary_lower) as salary_upper
	FROM unpacked

),

monthly_salary as (

	SELECT DISTINCT
		job_id,
		contract_type,
		CAST(
			CASE 
				WHEN salary_period='M' THEN salary_lower
				WHEN salary_period='D' THEN salary_lower * 21
				WHEN salary_period='H' THEN salary_lower * 8 * 21
				WHEN salary_period='A' THEN salary_lower / 12
			END as int
		) monthly_salary_lower,
		CAST(
			CASE 
				WHEN salary_period='M' THEN salary_upper
				WHEN salary_period='D' THEN salary_upper * 21
				WHEN salary_period='H' THEN salary_upper * 8 * 21
				WHEN salary_period='A' THEN salary_upper / 12
			END as int
		) as monthly_salary_upper
	FROM better_contracts

),

final as (

	SELECT
		job_id,
		contract_type,
		monthly_salary_lower,
		(monthly_salary_lower + monthly_salary_upper)/2 as monthly_salary_middle,
		monthly_salary_upper	
	FROM monthly_salary

)

SELECT * FROM final




















