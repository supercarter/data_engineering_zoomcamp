-- Question 3
SELECT count(1)
FROM green_taxi_data_gh
WHERE extract(day from date(lpep_pickup_datetime)) = 15
and extract(day from date(lpep_dropoff_datetime)) = 15



--Question 4
SELECT *
FROM green_taxi_data_gh
order by trip_distance desc
limit 1

--Question 5
SELECT 
    sum(case when passenger_count = 2 then 1 else 0 end) as passenger_2
    ,sum(case when passenger_count = 3 then 1 else 0 end) as passenger_3

FROM green_taxi_data_gh
WHERE extract(day from date(lpep_pickup_datetime)) = 1
and extract(day from date(lpep_dropoff_datetime)) = 1

-- question 6
select tip_amount,
		z.zone as pickupzone,
		z2.zone as dropoffzone
from green_taxi_data_gh
left join zone_lookup z on z.location_id = green_taxi_data_gh.pick_up_location
left join zone_lookup z2 on z2.location_id = green_taxi_data_gh.drop_off_location
where z.zone = 'Astoria'
order by tip_amount desc
limit 1
