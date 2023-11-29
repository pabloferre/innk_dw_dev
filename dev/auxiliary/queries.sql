--Query para sacar compañias que no sean demos--
select * from public.companies
where name not ilike '%demo%';

--Query para sacar formularios de compañia en particular, en esta
--en este caso compañia 17

select
cff.company_id as "company_id",
cff.form_id as "form_id",
cff.id as "field_id",
cff.title as "field_title",
cff.description as "field_description"
from company_form_fields cff
where cff.form_id in (
select
distinct(cf.id) as "form_id"
from companies c
join company_forms cf on c.id = cf.company_id
join company_form_fields cff on cf.id = cff.form_id
where c.id = 117
and cf.form_type = 0
and cff.id in (
select cff.id
from companies c
join company_form_fields cff on c.id = cff.company_id
inner join idea_field_answers ifa on cff.id = ifa.company_form_field_id
where c.id = 117
group by cff.id
order by count(ifa.id)
)
)
order by form_id asc;


--query para sacar las respuestas del formulario

select ifa.idea_id as "idea_id", cff.id as "field_id",ifa.answer as "field_answer", cff.company_id as "company_id", 
cff.title as "title", cff.description as "description", cff.form_field_id as "form_field_id", cff.form_id as "form_id",
ideas.title as "idea_name", ideas.description as "idea_description"
from idea_field_answers ifa 
join company_form_fields cff on ifa.company_form_field_id = cff.id 
join ideas on ideas.id = ifa.idea_id 
where cff.company_id = 117;

---query para sacar las respuestas del formulario y el formulario
WITH query1 AS (
    SELECT cff.company_id AS company_id,
           cff.form_id AS form_id,
           cff.id AS field_id,
           cff.title AS field_title,
           cff.description AS field_description
    FROM company_form_fields cff
    WHERE cff.form_id IN (
        SELECT DISTINCT(cf.id) AS form_id
        FROM companies c
        JOIN company_forms cf ON c.id = cf.company_id
        JOIN company_form_fields cff ON cf.id = cff.form_id
        WHERE c.id = 117
        AND cf.form_type = 0
        AND cff.id IN (
            SELECT cff.id
            FROM companies c
            JOIN company_form_fields cff ON c.id = cff.company_id
            INNER JOIN idea_field_answers ifa ON cff.id = ifa.company_form_field_id
            WHERE c.id = 117
            GROUP BY cff.id
            ORDER BY COUNT(ifa.id)
        )
    )
),
query2 AS (
    SELECT ifa.idea_id AS idea_id,
           cff.id AS field_id,
           ifa.answer AS field_answer
    FROM idea_field_answers ifa
    JOIN company_form_fields cff ON ifa.company_form_field_id = cff.id
    WHERE cff.company_id = 117
)
SELECT *
FROM query1
JOIN query2 ON query1.field_id = query2.field_id;


--query para sacar tabla de objetivos
select objectives.id, objectives.name, objectives.description, objectives_packages.company_id, objectives.objectives_package_id, objectives_packages.name,
objectives_packages.description, objectives_packages.objective_package_type 
from public.objectives
join public.objectives_packages on public.objectives_packages.id = objectives_package_id 

--Tabla de users

select users.id, users.company_id, users.email, users.first_name, users.last_name, users."position", 
users.contract_profile, users.area_id, ca."name" as "area", users.created_at, users.updated_at
from users
right join company_areas ca on ca.id = users.area_id 


--para eliminar duplicados

with duplicates as (
	SELECT id, ROW_NUMBER() OVER (PARTITION BY idea_db_id  ORDER BY idea_db_id) as dup 
	FROM public.dim_idea)
delete from public.dim_idea 
where id in (select id from duplicates where dup > 1);