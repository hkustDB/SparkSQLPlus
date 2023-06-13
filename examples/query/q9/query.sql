SELECT t1.T_ID, t1.T_DTS, t1.T_TT_ID, t1.T_TRADE_PRICE,
    t2.T_ID, t2.T_DTS, t2.T_TT_ID, t2.T_TRADE_PRICE,
    t1.T_S_SYMB, t1.T_CA_ID
FROM Trade t1, Trade t2
WHERE t1.T_TT_ID LIKE '%B%' AND t2.T_TT_ID LIKE '%S%'
    AND t1.T_CA_ID = t2.T_CA_ID AND t1.T_S_SYMB = t2.T_S_SYMB
    AND t1.T_DTS <= t2.T_DTS AND t1.T_DTS + interval '90' day >= t2.T_DTS
    AND t1.T_TRADE_PRICE * 1.2 < t2.T_TRADE_PRICE