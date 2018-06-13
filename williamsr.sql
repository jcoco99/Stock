select stockcode, max(logday.pricehigh) AS local_max from [stock].dbo.LogDay group by stockcode;

-- �ֱ�14�ϰ��� �ִ밡�� ���ϱ�
select stockcode, logdate, max(logday.pricehigh) over (partition by stockcode order by logdate asc rows 13 preceding) AS local_max from [stock].dbo.LogDay;

-- �ֱ�14�ϰ��� �ּҰ��� ���ϱ�

select stockcode, logdate, min(logday.pricelow) over (partition by stockcode order by logdate asc rows 13 preceding) AS local_max from [stock].dbo.LogDay;


-- �ֱ�14�ϰ��� �ִ밡��- �ּҰ��� ���ϱ�
select stockcode, logdate, min(logday.pricelow) over (partition by stockcode order by logdate asc rows 13 preceding) AS local_max from [stock].dbo.LogDay;

/*
update d1
set d1.high14day = (select stockcode, logdate, max(logday.pricehigh) over (partition by stockcode order by logdate asc rows 13 preceding) AS local_max from [stock].dbo.LogDay)
from [stock].dbo.LogDay d1
*/

--���̺� high14day, low14day, williamsr�� �߰��ϱ� ���ؼ� �ѹ��� ����
--ALTER TABLE dbo.LogDay ADD high14day FLOAT NULL, low14day FLOAT NULL, williamsr FLOAT NULL ; 

-- 14�� �ִ밡�� ������Ʈ
update d1
set d1.high14day = d2.local_max 
from [stock].dbo.LogDay d1 join 
(
	select stockcode, logdate, max(logday.pricehigh) over (partition by stockcode order by logdate asc rows 13 preceding) AS local_max
	from [stock].dbo.LogDay
) d2
on d1.stockcode = d2.stockcode and d1.logdate = d2.logdate; 

-- 14�� �ּҰ��� ������Ʈ
update d1
set d1.low14day = d2.local_min 
from [stock].dbo.LogDay d1 join 
(
	select stockcode, logdate, min(logday.pricelow) over (partition by stockcode order by logdate asc rows 13 preceding) AS local_min
	from [stock].dbo.LogDay
) d2
on d1.stockcode = d2.stockcode and d1.logdate = d2.logdate; 

-- ��������%14 ������Ʈ 
update d1
set d1.williamsr = 100 * ( d1.high14day - d1.priceClose ) / ( d1.high14day - d1.low14day)
from [stock].dbo.LogDay d1
where d1.high14day != d1.low14day

-- �츮������ 4��18�� ���� ���� williamsr ���ϱ�
select top(1) williamsr from [stock].dbo.LogDay where stockcode = 'A000030' and logdate < 20180418 order by logdate desc;

select logdate, williamsr from [stock].dbo.LogDay where stockcode = 'A000030';

-- ������ williamsR �� 80���� ũ��, ���� ���� ��������R �� ���� ���� : ���� 85 ���� 80.. �̷� ����
-- �̷������� ���������ؼ� ������ �ð��� �� ����� sql �켱����: marketCap
select stockcode, logdate, marketcap, volume, amount, priceClose as buyPrice, (select top(1) priceOpen from [stock].dbo.Logday d3 where d3.stockcode = d1.stockcode and d3.logdate > d1.logdate order by logdate asc ) as sellPrice
from [stock].dbo.LogDay d1 
where williamsr > 80 
	and amount > 5000000000
	and logdate > 20000417 and logdate < 20180504
	and williamsr < ( select top(1) williamsr from [stock].dbo.LogDay d2 where d2.stockcode = d1.stockcode and d2.logdate < d1.logdate order by logdate desc ) order by logdate, marketCap desc;

-- ������ williamsR �� 80���� ũ��, ���� ���� ��������R �� ���� ���� : ���� 85 ���� 80.. �̷� ����
-- �̷������� ���������ؼ� ������ �ð��� �� ����� sql �켱���� : priceClose
select stockcode, logdate, marketcap, volume, amount, priceClose as buyPrice, (select top(1) priceOpen from [stock].dbo.Logday d3 where d3.stockcode = d1.stockcode and d3.logdate > d1.logdate order by logdate asc ) as sellPrice
from [stock].dbo.LogDay d1 
where williamsr > 80 
	and amount > 5000000000
	and logdate > 20000417 and logdate < 20180504
	and williamsr < ( select top(1) williamsr from [stock].dbo.LogDay d2 where d2.stockcode = d1.stockcode and d2.logdate < d1.logdate order by logdate desc ) order by logdate, priceClose asc;



-- ������ williamsR �� 80���� ũ��, ���� ���� ��������R �� ���� ���� : ���� 85 ���� 80.. �̷� ����
-- �̷������� ���簡��,��ǥ����, �������������� ���
select stockcode, logdate, marketcap, volume, amount, priceClose as buyPrice, 
	(priceClose * 1.02 ) as targetPrice,
	(select top(1) logdate from [stock].dbo.Logday d3 where d3.stockcode = d1.stockcode and d3.logdate > d1.logdate order by logdate asc ) as nextDay,
	(select top(1) priceOpen from [stock].dbo.Logday d3 where d3.stockcode = d1.stockcode and d3.logdate > d1.logdate order by logdate asc ) as sellPrice
from [stock].dbo.LogDay d1 
where williamsr > 80 
	and amount > 5000000000
	and logdate > 20180417 and logdate < 20180504
	and williamsr < ( select top(1) williamsr from [stock].dbo.LogDay d2 where d2.stockcode = d1.stockcode and d2.logdate < d1.logdate order by logdate desc ) order by logdate, marketCap desc;


-- ������ williamsR �� 80���� ũ��, ���� ���� ��������R �� ���� ���� : ���� 85 ���� 80.. �̷� ����
-- �̷������� ���簡��,��ǥ����,10�ϵ�,Ÿ���ư���,�����ѳ�
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
		order by logdate asc offset 10 rows fetch next 1 rows only) as timecutDate --10�����ϵ��� ��
	from [stock].dbo.LogDay inner_d
	where williamsr > 80 
		and amount > 5000000000
		and logdate > 20170101 and logdate < 20180417
		and williamsr < ( 
			select top(1) williamsr from [stock].dbo.LogDay d2 where d2.stockcode = inner_d.stockcode and d2.logdate < inner_d.logdate order by logdate desc 
		) 
	order by logdate, marketCap desc
) d5 on d1.stockCode = d5.stockCode and d1.logDate = d5.LogDate order by d1.logdate, marketCap desc