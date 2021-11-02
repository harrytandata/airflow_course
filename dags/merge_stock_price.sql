-- update the existing rows
update stock_prices p, stock_prices_stage s
set  p.open_price = s.open_price,
    p.high_price = s.high_price,
    p.low_price = s.low_price,
    p.close_price = s.close_price,
    updated_at = now()
where p.ticker  = s.ticker
 and p.as_of_date = s.as_of_date;


-- inserting new rows 
 insert into stock_prices
 (ticker, as_of_date, open_price,high_price, low_price, close_price)
 select ticker, as_of_date, open_price,high_price, low_price, close_price
 from stock_prices_stage s 
    where not exists 
    (select 1 from stock_prices p
        where  p.ticker  = s.ticker
        and p.as_of_date = s.as_of_date);

-- truncate the stage table;
truncate table stock_prices_stage;