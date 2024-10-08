SELECT
    p.*,
    m.currency,
    m.first_traded
FROM
    {df} as p
LEFT JOIN
    {nodes.slv_stock_metadata} as m
ON
    p.symbol = m.symbol