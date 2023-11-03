---1
SELECT a.animal_type, COUNT(DISTINCT a.animal_id) as animal_count
FROM animal_dim a
INNER JOIN outcomes_fact o ON a.animal_id = o.animal_id
GROUP BY animal_type;


----2
select count(animal_id) as animals_morethan_onecount 
from(select a.animal_id from animal_dim a,
outcomes_fact o, outcometype_dim od where o.animal_id = a.animal_id 
and o.outcome_type_key = od.outcome_type_key and od.outcome_type is not null
group by a.animal_id having count(*)>1)as query;

----3
select t.month, count(t.month) as counter from date_dim t , outcometype_dim o , outcomes_fact o2 
where o2.outcome_type_key  = o.outcome_type_key and o2.date_key = t.date_key 
and o.outcome_type is not null
group by t.month
order by counter desc 
limit 5;

---1
SELECT a.animal_type, COUNT(DISTINCT a.animal_id) as animal_count
FROM animal_dim a
INNER JOIN outcomes_fact o ON a.animal_id = o.animal_id
GROUP BY animal_type;


----2
select count(animal_id) as animals_morethan_onecount 
from(select a.animal_id from animal_dim a,
outcomes_fact o, outcometype_dim od where o.animal_id = a.animal_id 
and o.outcome_type_key = od.outcome_type_key and od.outcome_type is not null
group by a.animal_id having count(*)>1)as query;

----3
select t.month, count(t.month) as counter from date_dim t , outcometype_dim o , outcomes_fact o2 
where o2.outcome_type_key  = o.outcome_type_key and o2.date_key = t.date_key 
and o.outcome_type is not null
group by t.month
order by counter desc 
limit 5;

---4
SELECT
    CASE
        WHEN CAST(age_years AS INTEGER) < 1 THEN 'Kitten'
        WHEN CAST(age_years AS INTEGER) >= 1 AND CAST(age_years AS INTEGER) <= 10 THEN 'Adult'
        WHEN CAST(age_years AS INTEGER) > 10 THEN 'Senior'
        else 'Other'
    END AS age_group,
    COUNT(*) AS count
FROM animal_dim da
WHERE da.animal_type = 'Cat'
GROUP BY age_group;

--4-2
SELECT
    CASE
        WHEN CAST(da.age_years AS INTEGER) < 1 THEN 'Kitten'
        WHEN CAST(da.age_years AS INTEGER) >= 1 AND CAST(da.age_years AS INTEGER) <= 10 THEN 'Adult'
        WHEN CAST(da.age_years AS INTEGER) > 10 THEN 'Senior'
    END AS age_group,
    COUNT(*) AS count
FROM animal_dim da
JOIN outcomes_fact fo ON da.animal_id = fo.animal_id
JOIN outcome_dim ot ON fo.outcome_id = ot.outcome_id
WHERE da.animal_type = 'Cat' and 
ot.outcome_type = 'Adopted'
GROUP BY age_group;



---5
SELECT 
    d.ts,
    COUNT(fo.outcome_key) AS outcomes,
    SUM(COUNT(fo.outcome_type_key)) OVER (ORDER BY d.ts) AS cumulative_outcomes
FROM date_dim d
LEFT JOIN outcomes_fact fo ON d.date_key = fo.date_key
GROUP BY d.ts
ORDER BY d.ts;
