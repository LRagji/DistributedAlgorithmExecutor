
Do we need a script for push ? its just lpush


local aIS = KEYS[1] --Instruction Set or Backlog Redis List
local aIP = KEYS[2] -- Instruction Pointer Redis Hashset
local aCI = KEYS[3] -- Completed Instructions Redis List
local aAI = KEYS[4] -- Async Instructions Redis List
local aParams = KEYS[5] -- Parameter for algorithm Redis Hashset

-- local stepName = ARGV[1]
local stepPayload = ARGV[1]
-- local stepMaximumExecutionTime = tonumber(ARGV[3])
-- local stepAsync = tonumber(ARGV[4])
-- local stepConsumers = cjson.decode(ARGV[5])

local tempTime = redis.call("TIME")
local currentTimestampInSeconds = tonumber(tempTime[1])

local returnArray = {}

redis.call("LPUSH",aIS,stepPayload);

table.insert(returnArray,1)
return returnArray