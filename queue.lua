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
        local litem = redis.call("lindex", acknowledged_list, i)
        if not litem then break end
        local litemlen = string.len(litem)

        local ts_string = nil
        for idx = 1, litemlen do
            if litem:byte(idx) == time_split_char then
                ts_string = string.sub(litem, 1, idx-1)
                break
            end
        end

        if ts_string ~= nil then
            local ts = tonumber(ts_string)
            if ts ~= nil and time_now_ts ~= nil and ts < time_now_ts then
                -- we need to remove this item from the ack list and then return it
                local data_without_ts = string.sub(litem, string.len(ts_string)+2)
                redis.call("lrem", acknowledged_list, 1, litem)
                return data_without_ts
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
