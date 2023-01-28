local list_name = KEYS[1]
local acknowledged_list = KEYS[2]
local item_expiration_timestamp_str = KEYS[3]
local list_expiration_time = tonumber(KEYS[4])
local pop_command = KEYS[5]
local push_command = KEYS[6]
local element = redis.call(pop_command, list_name)
if element then
    local prefix = item_expiration_timestamp_str .. "|"
    redis.call(push_command, acknowledged_list, prefix .. element)
    redis.call("expire", acknowledged_list, list_expiration_time)
    redis.call("ltrim", acknowledged_list, 0, 10000)
    return element
end
return nil
