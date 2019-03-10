with statuses as (
select
	id_application,
	[time] as 'time_status',
	name,
	reason,
	description,
	lag(name) over (partition by id_application order by [time] asc) as 'prev_name',
	lag(reason) over (partition by id_application order by [time] asc) as 'prev_reason',
	lag(description) over (partition by id_application order by [time] asc) as 'prev_description',
	lag([time]) over (partition by id_application order by [time] asc) as 'prev_time_status',
	lead(name) over (partition by id_application order by [time] asc) as 'next_name',
	lead(reason) over (partition by id_application order by [time] asc) as 'next_reason',
	lead(description) over (partition by id_application order by [time] asc) as 'next_description',
	lead([time]) over (partition by id_application order by [time] asc) as 'next_time_status',
	case
		when name not in ('SUCCESS', 'CANCELED', 'REJECTED') then
			datediff(minute,
					[time],
					coalesce(lead([time]) over (partition by id_application order by [time] asc), CURRENT_TIMESTAMP)
					)
	end as 'processing_in_minutes'
from
	qf_status
),
statuses_sla as (
select
	id_application,
	time_status,
	name,
	reason,
	description,
	prev_name,
	prev_reason,
	prev_description,
	prev_time_status,
	next_name,
	next_reason,
	next_description,
	next_time_status,
	processing_in_minutes,
	case
		when name = 'NEW' and datediff(minute, time_status, next_time_status) < 5  then 'менее 5 минут'
		when name = 'NEW' and datediff(minute, time_status, next_time_status) < 10 then 'от 5 до 10 минут'
		when name = 'NEW' and datediff(minute, time_status, next_time_status) > 9  then 'более 10 минут'
		when name in ('CUSTOMER_FILLING', 'EMPLOYEE_FILLING', 'VALID', 'VALIDATING')
					and datediff(minute, time_status, next_time_status) < 30  then 'менее 30 минут'
		when name in ('CUSTOMER_FILLING', 'EMPLOYEE_FILLING', 'VALID', 'VALIDATING')
					and datediff(minute, time_status, next_time_status) < 90 then 'от 30 до 90 минут'
		when name in ('CUSTOMER_FILLING', 'EMPLOYEE_FILLING', 'VALID', 'VALIDATING')
					and datediff(minute, time_status, next_time_status) > 89  then 'более 90 минут'
	end as 'sla'
from
	statuses
),
app as (
select
     id,
     id_form,
     status,
     description,
     created_at,
     updated_at,
     title
from
	qf_application),
regno as (
select
	id,
	regno,
	phone
from
	lkdb01.quiz.qf_form
),
enrich_app as(
select
     l.id,
     l.id_form,
     l.status,
     l.description,
     l.created_at,
     l.updated_at,
     l.title,
     r.regno,
     r.phone
from
	app l
left join
	regno r
on
	l.id_form = r.id
),
enrich_statuses as(
select
	l.id_application,
	l.time_status,
	l.name,
	l.reason,
	l.description,
	l.prev_name,
	l.prev_reason,
	l.prev_description,
	l.prev_time_status,
	l.next_name,
	l.next_reason,
	l.next_description,
	l.next_time_status,
	l.processing_in_minutes as 'processing_stage_in_minutes',
	case
		when name='COMPLETE' and datediff(hour, r.created_at, l.time_status) < 24 then 'менее 24ч'
		when name='COMPLETE' and datediff(hour, r.created_at, l.time_status) < 72 then 'от 24ч до 72ч'
		when name='COMPLETE' and datediff(hour, r.created_at, l.time_status) > 71 then 'более 72ч'
		else l.sla
	end as 'sla',
	datediff(hour, r.created_at, l.time_status) as 'processing_application_in_hours',
	r.id_form,
    r.status as 'last_status',
    r.description as 'last_description',
    r.created_at as 'application_created_at',
    r.updated_at as 'application_updated_at',
    r.title,
    r.regno,
    r.phone
from
	statuses_sla l
left join
	enrich_app r
on
	l.id_application = r.id
),
qf_lines as (
select
	id_form,
	name,
	value
from
	qf_line
where
	name in ('okopf_name',
			 'location_address-region_name',
			 'location_address-city_name',
			 'location_address-city_type',
			 'registration_address-region_name',
			 'registration_address-region_type',
			 'registration_address-city_name',
			 'registration_address-city_type',
			 'city_name',
			 'activity_classifier_0-name',
			 'service_set',
			 'branch_name',
			 'office_name',
			 'bank_service_0-name',
			 'bank_service_1-name',
			 'staff_number')
),
pivot_lines as (
select *
from
	qf_lines
	pivot(max(value) for name in ([okopf_name],
								 [location_address-region_name],
								 [location_address-city_name],
								 [location_address-city_type],
								 [registration_address-region_name],
								 [registration_address-region_type],
								 [registration_address-city_name],
								 [registration_address-city_type],
								 [city_name],
								 [activity_classifier_0-name],
								 [service_set],
								 [branch_name],
								 [office_name],
								 [bank_service_0-name],
								 [bank_service_1-name],
								 [staff_number])) as d
),
res as (
select
	l.id_application,
	l.time_status,
	l.name,
	l.reason,
	l.description,
	l.prev_name,
	l.prev_reason,
	l.prev_description,
	l.prev_time_status,
	l.next_name,
	l.next_reason,
	l.next_description,
	l.next_time_status,
	l.processing_stage_in_minutes,
	l.sla,
	l.processing_application_in_hours,
	l.id_form,
  l.last_status,
  l.last_description,
  l.application_created_at,
  l.application_updated_at,
  l.title,
  l.regno,
	l.phone,
  r.okopf_name,
	r.[location_address-region_name] as 'location_address_region_name',
	r.[location_address-city_name] as 'location_address_city_name',
	r.[location_address-city_type] as 'location_address_city_type',
	r.[registration_address-region_name] as 'registration_address_region_name',
	r.[registration_address-region_type] as 'registration_address_region_type',
	r.[registration_address-city_name] as 'registration_address_city_name',
	r.[registration_address-city_type] as 'registration_address_city_type',
	r.city_name,
	r.[activity_classifier_0-name] as 'activity_classifier_0_name',
	r.service_set,
	r.branch_name,
	r.office_name,
	r.[bank_service_0-name] as 'bank_service_0_name',
	r.[bank_service_1-name] as 'bank_service_1_name',
	r.staff_number
from
	enrich_statuses l
left join
	pivot_lines r
on
	l.id_form = r.id_form
)
select * from res;