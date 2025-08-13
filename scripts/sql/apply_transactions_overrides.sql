--Updated Jose Fernandez transaction to reflect his passing
UPDATE transactions
set typecode = 'DEC',
typedesc = 'Deceased'
where trans_id = 245302;

/*Deleted transaction of Will Banfield being put on IL on 2020-07-06.
He was at the alternate site for the Marlins. The MLB statsAPI has him
being put on the IL as a Major League transaction, but him being 
activated to the alternate site is not Major League transaction.
So, I am deleting the transaction of him being put on the injured list.
*/
delete from transactions
where trans_id = 446539;


/*Added transaction for Tyler Soderstrom being activated
after his IL stint from 2024-07-11 to 2024-09-13
*/
INSERT INTO transactions (trans_id, player_id, player, typecode, typedesc, team, date, effective_date, descr)
VALUES (999999, 691016, 'Tyler Soderstrom', 'SC', 'Status Change', 'Oakland Athletics', '2024-09-14', 
'2024-09-14', 'Oakland Athletics activated 1B Tyler Soderstrom from 10-day injured list.');


/*Effective date was '0205-02-2020'*/
UPDATE transactions
set effective_date = '2025-02-20'
where trans_id = 816212;


/*Added transaction for Jacob Wilson being activated
after his IL stint from 2024-07-20 to 2024-08-26
*/
INSERT INTO transactions (trans_id, player_id, player, typecode, typedesc, team, date, effective_date, descr)
VALUES (999998, 805779, 'Jacob Wilson', 'SC', 'Status Change', 'Oakland Athletics', '2024-08-27', 
'2024-08-27', 'Oakland Athletics activated SS Jacob Wilson from 10-day injured list.');


/* Effective date of the Chris Archer being placed on IL was 2013-06-05, changing it to 2018-06-05
and deleting transaciton of him being activated on 2018-06-05
*/
UPDATE transactions
set effective_date = '2018-06-05'
where trans_id = 358140;

delete
from transactions
where trans_id = 367424;

/* Effective date of the Hisashi Iwakuma was 2014-04-21, changing it to 2015-04-21
*/
UPDATE transactions
set effective_date = '2015-04-21'
where trans_id = 223188;