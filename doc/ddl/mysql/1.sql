SELECT
    date_add('2020-03-31',1) AS mm,
    sum(CASE WHEN order_type IN ('新增') THEN total_seats ELSE 0 END) AS 'new_seats',
    sum(CASE WHEN order_type IN ('增购','云SDK') THEN total_seats ELSE 0 END) AS 'expand_seats',
    sum(total_seats) AS total_seats
FROM
    orders.orders
WHERE
        order_start_date <= '2020-03-31'
  AND
        order_end_date >= '2020-03-31'
  AND
    order_type NOT REGEXP '升级'
GROUP BY 1;

select month('2020-12-01') as dt;

create table call_result(
    mm varchar(10),
    new_seats int(6),
    expand_seats int(6),
    total_seats int(6)
);


select concat(left('2020-12-01',7),'-01') as dt;
DELIMITER $$
DROP PROCEDURE IF EXISTS test_mysql_while_loop$$
CREATE PROCEDURE test_mysql_while_loop()
BEGIN
    DECLARE x VARCHAR(10);
    SET x = '2017-02-01';
    WHILE x <= '2020-11-30' DO
        SET month_start = concat(left(x,7),'-01');
        SET month_end = concat(left(x,7),'-31');
        delete from call_result where mm=x;
        insert into call_result 
        SELECT date_add(x,1) AS mm, sum(CASE WHEN order_type IN ('新增') THEN total_seats ELSE 0 END) AS 'new_seats', sum(CASE WHEN order_type IN ('增购','云SDK') THEN total_seats ELSE 0 END) AS 'expand_seats', sum(total_seats) AS total_seats FROM orders.orders
        WHERE order_start_date <= month_start
          AND
                order_end_date >= month_end
          AND
            order_type NOT REGEXP '升级'
        GROUP BY 1;
        SET x = concat(left(date_add(x,31),9),'-01');
    END WHILE;
END$$
DELIMITER ;

select date_add(now(),INTERVAL 1 DAY) as dt;
select concat(left(date_add(str_to_date('2016-01-02', '%Y-%m-%d'),INTERVAL 1 MONTH),7),'-01') as dt;

create table if not exists call_result( mm varchar(10), new_seats int(6), expand_seats int(6), total_seats int(6));

DELIMITER $$
DROP PROCEDURE IF EXISTS test_mysql_while_loop$$
CREATE PROCEDURE test_mysql_while_loop(IN start_month VARBINARY(7),IN end_month VARBINARY(7))
BEGIN
    DECLARE x VARCHAR(10);
    SET x = (select concat(start_month,'-01'));
    WHILE x <= (select concat(end_month,'-01')) DO
        delete from call_result where mm=x;
        insert into call_result SELECT date_add(x,1) AS mm, sum(CASE WHEN order_type IN ('新增') THEN total_seats ELSE 0 END) AS 'new_seats', sum(CASE WHEN order_type IN ('增购','云SDK') THEN total_seats ELSE 0 END) AS 'expand_seats', sum(total_seats) AS total_seats FROM orders.orders WHERE order_end_date >=  concat(left(x,7),'-01') AND order_start_date <= concat(left(x,7),'-31') AND order_type NOT REGEXP '升级' GROUP BY 1 ;
        select x;
        set x = (select date_add(str_to_date(x, '%Y-%m-%d'),INTERVAL 1 MONTH));
    END WHILE;
END$$
DELIMITER ;

call test_mysql_while_loop('2017-02','2017-04');

select * from call_result;