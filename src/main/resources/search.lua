local searchKey = ARGV[1]  -- The search keyword

local results = {}
local orders = redis.call('HGETALL', KEYS[1])  -- Get all orders from the Hash

local i = 1
while i <= #orders do
    local json = orders[i + 1]  -- JSON object for the order
    i = i + 2  -- Increment index by 2 to get to the next field

    -- Check if the JSON object contains the search keyword
    if string.find(json, searchKey) then
        table.insert(results, json)  -- Add matching JSON objects to the results
    end
end

return results
