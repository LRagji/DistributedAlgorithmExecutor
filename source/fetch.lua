local aIS = KEYS[1] --Instruction Set or Backlog Redis List
local aIP = KEYS[2] -- Instruction Pointer Redis Hashset
local aCI = KEYS[3] -- Completed Instructions Redis List
local aAI = KEYS[4] -- Async Instructions Redis List
local aParams = KEYS[5] -- Parameter for algorithm Redis Hashset

local consumerName = ARGV[1]
local returnArray = {}
local tempTime = redis.call("TIME")
local currentTimestampInSeconds = tonumber(tempTime[1])

local stepPeek = redis.call("LRANGE",aIS,-1,-1)
if(stepPeek[1] ~= nil) then
    stepPeek = cjson.decode(stepPeek[1])
else
    stepPeek = nil
end
local IPExists = redis.call("EXISTS",aIP)
--Start state
--Parameter fetch state
--Timeout state
--Async state
table.insert(returnArray,1)
table.insert(returnArray,stepPeek["C"])
return returnArray