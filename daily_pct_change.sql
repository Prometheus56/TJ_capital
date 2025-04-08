CREATE OR REPLACE FUNCTION get_bitcoin_daily_percent_change()
RETURNS TABLE(
    date date,
    bitcoin numeric,
    percent_change numeric
) AS $$
BEGIN
    RETURN QUERY
    SELECT a.date,
           a.bitcoin,
           CASE 
               WHEN b.bitcoin IS NULL OR b.bitcoin = 0 
               THEN NULL 
               ELSE ((a.bitcoin - b.bitcoin) / b.bitcoin) * 100
           END AS percent_change
    FROM chains a
    LEFT JOIN chains b ON a.date = b.date + INTERVAL '1 day'
    ORDER BY a.date;
END;
$$ LANGUAGE plpgsql;
