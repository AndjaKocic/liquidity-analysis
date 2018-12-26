-- problem 1.3

select seccode, count(seccode) from trade_log
    where EXISTS (select * from ordinary_stock_order_log where ordinary_stock_order_log.seccode = seccode)
    group by seccode
    order by count(seccode)
    desc limit 1;
