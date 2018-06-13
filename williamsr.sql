select stockcode, max(logday.pricehigh) AS local_max from [stock].dbo.LogDay group by stockcode;

-- 최근14일간의 최대가격 구하기
select stockcode, logdate, max(logday.pricehigh) over (partition by stockcode order by logdate asc rows 13 preceding) AS local_max from [stock].dbo.LogDay;

-- 최근14일간의 최소가격 구하기

select stockcode, logdate, min(logday.pricelow) over (partition by stockcode order by logdate asc rows 13 preceding) AS local_max from [stock].dbo.LogDay;


-- 최근14일간의 최대가격- 최소가격 구하기
select stockcode, logdate, min(logday.pricelow) over (partition by stockcode order by logdate asc rows 13 preceding) AS local_max from [stock].dbo.LogDay;

/*
update d1
set d1.high14day = (select stockcode, logdate, max(logday.pricehigh) over (partition by stockcode order by logdate asc rows 13 preceding) AS local_max from [stock].dbo.LogDay)
from [stock].dbo.LogDay d1
*/

--테이블에 high14day, low14day, williamsr을 추가하기 위해서 한번만 수행
--ALTER TABLE dbo.LogDay ADD high14day FLOAT NULL, low14day FLOAT NULL, williamsr FLOAT NULL ; 

-- 14일 최대가격 업데이트
update d1
set d1.high14day = d2.local_max 
from [stock].dbo.LogDay d1 join 
(
	select stockcode, logdate, max(logday.pricehigh) over (partition by stockcode order by logdate asc rows 13 preceding) AS local_max
	from [stock].dbo.LogDay
) d2
on d1.stockcode = d2.stockcode and d1.logdate = d2.logdate; 

-- 14일 최소가격 업데이트
update d1
set d1.low14day = d2.local_min 
from [stock].dbo.LogDay d1 join 
(
	select stockcode, logdate, min(logday.pricelow) over (partition by stockcode order by logdate asc rows 13 preceding) AS local_min
	from [stock].dbo.LogDay
) d2
on d1.stockcode = d2.stockcode and d1.logdate = d2.logdate; 

-- 윌리엄스%14 업데이트 
update d1
set d1.williamsr = 100 * ( d1.high14day - d1.priceClose ) / ( d1.high14day - d1.low14day)
from [stock].dbo.LogDay d1
where d1.high14day != d1.low14day

-- 우리은행의 4월18일 기준 전일 williamsr 구하기
select top(1) williamsr from [stock].dbo.LogDay where stockcode = 'A000030' and logdate < 20180418 order by logdate desc;

select logdate, williamsr from [stock].dbo.LogDay where stockcode = 'A000030';

-- 당일의 williamsR 이 80보다 크고, 전일 보다 윌리엄스R 이 작은 종목 : 전일 85 당일 80.. 이런 종목
-- 이런종목을 종가베팅해서 다음날 시가에 판 경우의 sql 우선순위: marketCap
select stockcode, logdate, marketcap, volume, amount, priceClose as buyPrice, (select top(1) priceOpen from [stock].dbo.Logday d3 where d3.stockcode = d1.stockcode and d3.logdate > d1.logdate order by logdate asc ) as sellPrice
from [stock].dbo.LogDay d1 
where williamsr > 80 
	and amount > 5000000000
	and logdate > 20000417 and logdate < 20180504
	and williamsr < ( select top(1) williamsr from [stock].dbo.LogDay d2 where d2.stockcode = d1.stockcode and d2.logdate < d1.logdate order by logdate desc ) order by logdate, marketCap desc;

-- 당일의 williamsR 이 80보다 크고, 전일 보다 윌리엄스R 이 작은 종목 : 전일 85 당일 80.. 이런 종목
-- 이런종목을 종가베팅해서 다음날 시가에 판 경우의 sql 우선순위 : priceClose
select stockcode, logdate, marketcap, volume, amount, priceClose as buyPrice, (select top(1) priceOpen from [stock].dbo.Logday d3 where d3.stockcode = d1.stockcode and d3.logdate > d1.logdate order by logdate asc ) as sellPrice
from [stock].dbo.LogDay d1 
where williamsr > 80 
	and amount > 5000000000
	and logdate > 20000417 and logdate < 20180504
	and williamsr < ( select top(1) williamsr from [stock].dbo.LogDay d2 where d2.stockcode = d1.stockcode and d2.logdate < d1.logdate order by logdate desc ) order by logdate, priceClose asc;



-- 당일의 williamsR 이 80보다 크고, 전일 보다 윌리엄스R 이 작은 종목 : 전일 85 당일 80.. 이런 종목
-- 이런종목의 현재가격,목표가격, 다음날정보까지 출력
select stockcode, logdate, marketcap, volume, amount, priceClose as buyPrice, 
	(priceClose * 1.02 ) as targetPrice,
	(select top(1) logdate from [stock].dbo.Logday d3 where d3.stockcode = d1.stockcode and d3.logdate > d1.logdate order by logdate asc ) as nextDay,
	(select top(1) priceOpen from [stock].dbo.Logday d3 where d3.stockcode = d1.stockcode and d3.logdate > d1.logdate order by logdate asc ) as sellPrice
from [stock].dbo.LogDay d1 
where williamsr > 80 
	and amount > 5000000000
	and logdate > 20180417 and logdate < 20180504
	and williamsr < ( select top(1) williamsr from [stock].dbo.LogDay d2 where d2.stockcode = d1.stockcode and d2.logdate < d1.logdate order by logdate desc ) order by logdate, marketCap desc;


-- 당일의 williamsR 이 80보다 크고, 전일 보다 윌리엄스R 이 작은 종목 : 전일 85 당일 80.. 이런 종목
-- 이런종목의 현재가격,목표가격,10일뒤,타임컷가격,익절한날
select d1.stockcode, d1.logdate, d1.marketcap, d1.volume, d1.amount, priceClose as buyPrice,
	d5.nextDay,
	d5.timecutDate,
	(select priceOpen 
		from [stock].dbo.Logday d3 
		where d3.stockcode = d1.stockcode and d3.logdate > d1.logdate 
		order by logdate asc offset 10 rows fetch next 1 rows only) as timecutPrice,
	d5.targetPrice,
	(select top(1) logdate
		from [stock].dbo.LogDay d3
		where d3.stockcode = d1.stockcode and d3.priceHigh > d5.targetPrice and d3.logdate between nextDay and timecutDate order by d3.logdate asc) as gainSellDate
from [stock].dbo.LogDay d1 join 
(
	select top(100) percent stockcode, logdate, marketcap, volume, amount,
	(priceClose * 1.01 ) as targetPrice,
	(select top(1) logdate 
		from [stock].dbo.Logday d3 
		where d3.stockcode = inner_d.stockcode and d3.logdate > inner_d.logdate 
		order by logdate asc ) as nextDay,
	(select logdate 
		from [stock].dbo.Logday d3 
		where d3.stockcode = inner_d.stockcode and d3.logdate > inner_d.logdate 
		order by logdate asc offset 10 rows fetch next 1 rows only) as timecutDate --10영업일뒤의 날
	from [stock].dbo.LogDay inner_d
	where williamsr > 80 
		and amount > 5000000000
		and logdate > 20170101 and logdate < 20180417
		and williamsr < ( 
			select top(1) williamsr from [stock].dbo.LogDay d2 where d2.stockcode = inner_d.stockcode and d2.logdate < inner_d.logdate order by logdate desc 
		) 
	order by logdate, marketCap desc
) d5 on d1.stockCode = d5.stockCode and d1.logDate = d5.LogDate order by d1.logdate, marketCap desc