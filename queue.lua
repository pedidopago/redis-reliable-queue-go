local list_name = KEYS[1]
local acknowledged_list = KEYS[2]
local unix_time_now_str = KEYS[3]
local item_expiration_timestamp_str = KEYS[4]
local list_expiration_time = tonumber(KEYS[5])
local pop_command = KEYS[6]
local ack_list_limit = tonumber(KEYS[7])

local time_split_char = string.byte("|")

-- try to get expired item from ACK

local ack_len = redis.call("llen", acknowledged_list)
local time_now_ts = tonumber(unix_time_now_str)

if ack_len > 0 then
    for i = 0, ack_len, 1 do
        local raw_ack_item = redis.call("lindex", acknowledged_list, i)
        if not raw_ack_item then break end
        local item_str_len = string.len(raw_ack_item)

        local ts_string = nil
        for idx = 1, item_str_len do
            if raw_ack_item:byte(idx) == time_split_char then
                ts_string = string.sub(raw_ack_item, 1, idx-1)
                break
            end
        end

        if ts_string == nil then
            -- this should NEVER happen, unless someone manually added garbage inside this list
            -- add this item to a debug list
            redis.call("rpush", acknowledged_list .. "-bug", raw_ack_item)
            redis.call("ltrim", acknowledged_list .. "-bug", 0, 10)
            redis.call("lrem", acknowledged_list, 1, raw_ack_item)
            break
        end
        
        local ts = tonumber(ts_string)
        if ts ~= nil and time_now_ts ~= nil then
            if ts < time_now_ts then
                -- we need to remove this item from the ack list and then return it
                local data_without_ts = string.sub(raw_ack_item, string.len(ts_string)+2)
                redis.call("lrem", acknowledged_list, 1, raw_ack_item)
                return data_without_ts
            else
                -- this is the oldest item in the list and it did not expire,
                -- so it's safe to exit this lindex loop here
                break
            end
        end
    end
end

-- ack is clear; get from the main list

local element = redis.call(pop_command, list_name)
if element then
    local prefix = item_expiration_timestamp_str .. "|"
    redis.call("rpush", acknowledged_list, prefix .. element)
    redis.call("expire", acknowledged_list, list_expiration_time)
    redis.call("ltrim", acknowledged_list, 0, ack_list_limit)
    return element
end
return nil
