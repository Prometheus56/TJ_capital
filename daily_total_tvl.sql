CREATE OR REPLACE FUNCTION get_daily_total_tvl()
RETURNS TABLE(
    date date,
    total_tvl numeric
) AS $$
BEGIN
    RETURN QUERY
    SELECT date, bitcoin + ethereum + solana AS total_tvl
    FROM chains
    ORDER BY date;
END;
$$ LANGUAGE plpgsql;
