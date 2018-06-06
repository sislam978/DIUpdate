//Recurring: Monthly: FUND & Equity Dividends update

SELECT b.* FROM zsenia_fund_dividends a, zsenia_fund_dividends b WHERE a.kkr_company_id = b.kkr_company_id AND a.amount = b.amount  and a.frequency = b.frequency and a.date = b.date and a.divflag = b.divflag and a.divtype=b.divtype and a.id<>b.id order by kkr_company_id;


//delete the one duplicates
DELETE b FROM zsenia_fund_dividends a, zsenia_fund_dividends b WHERE a.kkr_company_id = b.kkr_company_id  AND a.amount = b.amount  and a.frequency = b.frequency and a.date = b.date and a.divflag = b.divflag and a.divtype=b.divtype and a.id>b.id