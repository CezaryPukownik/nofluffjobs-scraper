with salary as (

	SELECT * FROM {{ ref('fct_job_has_salary') }}

),

final as (

	SELECT
		job_id,
		CAST(AVG(monthly_salary_middle) filter (where contract_type='B2B')  as int) as b2b_salary,
		CAST(AVG(monthly_salary_middle) filter (where contract_type='UoP')  as int) as uop_salary,
		CAST(AVG(monthly_salary_middle) filter (where contract_type='UZ') as int) as uz_salary

	FROM salary
	GROUP BY job_id

)

SELECT * FROM final
