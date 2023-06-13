CREATE TABLE Trade (
    T_ID BIGINT,
    T_DTS TIMESTAMP,
    T_TT_ID VARCHAR,
    T_S_SYMB VARCHAR,
    T_CA_ID BIGINT,
    T_TRADE_PRICE DOUBLE
) WITH (
    'path' = 'examples/data/trade.dat'
)