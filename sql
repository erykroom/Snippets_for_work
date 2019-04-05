-- select all records created last month
SELECT * 
  FROM table 
 WHERE created_at::date >= date_trunc('MONTH', current_date - INTERVAL '1 MONTH' )::DATE
   AND created_at::date <= (date_trunc('MONTH', current_date - INTERVAL '1 MONTH' ) + INTERVAL '1 MONTH - 1 day')::DATE;

-- list all the tables with a name that matches a pattern e.g. table_*, ignoring case
SELECT *
  FROM information_schema.tables 
 WHERE table_name ILIKE '%table%'
 
-- group by week
  SELECT date_trunc('week', created_at::TIMESTAMP)::DATE AS "week", 
         count(id) AS id_count
    FROM my_table 
   WHERE created_at is NOT NULL
GROUP BY week
ORDER BY week

-- cumulative sum
with sub_query AS (    
  SELECT to_char(t.created_at::DATE,'YYYY/MM') as month, 
	       sum(a.value::FLOAT) AS value
    FROM my_table t 
   WHERE t.condition = true
GROUP BY to_char(t.created_at::DATE,'YYYY/MM')
ORDER BY to_char(t.created_at::DATE,'YYYY/MM'))
	SELECT month,
         value,
         sum(value) over (order by month) as cumulative_value
    FROM sub_query

-- how to get around not using "distinct on (A, B, C)" as redshift doesn't support it
SELECT * 
  FROM
  (SELECT  created_at, 
   	   id, 
           name, 
   	   -- want to select distinct id and name, but the first created at date
	   rank() OVER (PARTITION BY id, name ORDER BY created_at asc) AS parent_id_ranked
   FROM my_table) AS ranked
  WHERE ranked.parent_id_ranked = 1

-- how to create a column with all days/months/years in a period e.g. 01-01-2010 to one year from now. 
-- This is useful for using with a left join to other queries so that no days/months are missed out in the final query
-- replace '1 month'::interval -> '1 day'::interval and 'month' -> 'day' to get daily series
SELECT date_trunc('month', mm)::date as "month"
FROM generate_series
  ( '2010-01-01'::timestamp
  , (current_date + interval '1 year')::timestamp
  , '1 month'::interval) mm
 
-- show table/graph for specific values 
SELECT  column1 as "year", 
	column2 as "amount" 
FROM (
	VALUES  (2009, 3300495.79), 
		(2010, 2894192.76), 
		(2011, 3195881.66), 
		(2012, 2682215.17), 
		(2013, 2163819.42), 
		(2014, 2567560.77)
     ) as vals


-- starting to learn how to query jsonb
-- https://www.postgresql.org/docs/current/static/functions-json.html#FUNCTIONS-JSON-OP-TABLE
-- http://stackoverflow.com/questions/22736742/query-for-array-elements-inside-json-type
SELECT  blob -> 'assets' -> 1 -> 'year', 
	blob -> 'assets' -> 1 -> 'staff' 
FROM data_sources
WHERE novicap_id = 'ESB86675410'
AND blob ->> 'found' = 'true'
AND blob ->'assets' @> '[{"year":"2014"}]';

with ass AS (
	SELECT blob -> 'assets' as blob 
	FROM data_sources 
	WHERE novicap_id = 'ESB86675410')
SELECT  blob -> 0 -> 'year' as year, 
	blob -> 0 -> 'staff' as staff
FROM ass;

# List of new investors, based on deposits
select distinct on (e.investor_id, i.name, u.email)
               to_char(e.created_at::date, 'YYYY/MM') as month,
               e.investor_id as inv_id,
               i.name,
               u.email,
               u.first_name,
               u.last_name
        from explanations e
          inner join investors i on i.id = e.investor_id
          INNER JOIN investor_memberships im on im.investor_id = i.id
          inner join users u on u.id = im.user_id
        where e.explained_as = 'investor_deposit'
        and u.admin = false
          and e.created_at::date <= current_date
        order by inv_id, i.name, u.email, month asc
