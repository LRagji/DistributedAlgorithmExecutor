-- Return Codes
-- -100 = No steps for current consumer
-- 0 = No steps found
-- (nil) = Unknown error
-- 1 = Step found
-- 2 = current step timedout so reacquired
-- -1 = Current Async step pushed aside in next fetch will get next step.
local aIS = KEYS[1] -- Instruction Set or Backlog Redis List
local aIP = KEYS[2] -- Instruction Pointer Redis Hashset
local aCI = KEYS[3] -- Completed Instructions Redis List
local aAI = KEYS[4] -- Async Instructions Redis List
local aParams = KEYS[5] -- Parameter for algorithm Redis Hashset

local consumerName = ARGV[1]
local returnArray = {}
local tempTime = redis.call("TIME")
local currentTimestampInSeconds = tonumber(tempTime[1])

local IPExists = redis.call("EXISTS", aIP)
-- Functions
local function contains(tab, val)
    for index, value in ipairs(tab) do
        if value == val then
            return true
        end
    end

    return false
end

-- Instruction Pointer exists
if (IPExists == 1) then
    local stepPeek = {}
    -- Fetch Instruction Pointer
    local temp = redis.call("HGETALL", aIP)
    if (type(temp) == "boolean") then
        stepPeek = nil
    else
        -- Convert IP into IRedisStep Interface
        for index = 1, #temp, 2 do
            local propName = temp[index]
            local propValue = temp[index + 1]
            if (propName == "args" or propName == "consumers") then
                propValue = cjson.decode(propValue)
            end
            if (propName == "start" or propName == "maximumTime" or propName == "argsFetchCompleted" or propName ==
                "async") then
                propValue = tonumber(propValue)
            end
            stepPeek[propName] = propValue
        end

        if (contains(stepPeek["consumers"], consumerName) or #stepPeek["consumers"] == 0) then
            if (stepPeek["argsFetchCompleted"] == 0) then
                -- Argument fetch state and
                -- Fetch Arguments
                local argsFetchCompleted = 1
                local argsFetched = {};
                for index, arg in pairs(stepPeek["args"]) do
                    if (arg["type"] == "Reference") then
                        local paramValue = redis.call("HGET", aParams, arg["value"])
                        if (type(paramValue) == "boolean") then
                            argsFetchCompleted = 0
                        else
                            argsFetchCompleted = 1
                            argsFetched[arg["value"]] = paramValue
                        end
                    end
                end

                -- Populate step if all args are found
                if (argsFetchCompleted == 1) then
                    for index, arg in pairs(stepPeek["args"]) do
                        if (arg["type"] == "Reference") then
                            arg["value"] = argsFetched[arg["value"]]
                            arg["type"] = "Resolved"
                        end
                    end
                    table.insert(returnArray, 1)
                    stepPeek["consumer"] = consumerName
                    stepPeek["start"] = currentTimestampInSeconds
                    table.insert(returnArray, cjson.encode(stepPeek))
                else
                    table.insert(returnArray, 0)
                end

                -- Populate Completed Instruction log
                local log = {}
                log["time"] = currentTimestampInSeconds
                log["consumer"] = consumerName
                log["state"] = "Fetch Args"
                log["stack"] = redis.call("DUMP", aIP)
                redis.call("LPUSH", aCI, cjson.encode(log))

                -- Delete previous instruction Pointer
                redis.call("DEL", aIP)

                -- Populate Instruction Pointer & Pop backlog
                redis.call("HSET", aIP, "start", stepPeek["start"], "consumer", stepPeek["consumer"],
                    "argsFetchCompleted", argsFetchCompleted, "consumers", cjson.encode(stepPeek["consumers"]), "async",
                    stepPeek["async"], "maximumTime", stepPeek["maximumTime"], "opCode", stepPeek["opCode"], "stepName",
                    stepPeek["stepName"], "args", cjson.encode(stepPeek["args"])) -- Populate Instruction Pointer

                return returnArray

            else
                if (stepPeek["argsFetchCompleted"] == 1 and (currentTimestampInSeconds - stepPeek["start"]) >
                    stepPeek["maximumTime"]) then
                    -- Timeout state
                    table.insert(returnArray, 2)
                    stepPeek["consumer"] = consumerName
                    stepPeek["start"] = currentTimestampInSeconds
                    table.insert(returnArray, cjson.encode(stepPeek))

                    -- Populate Completed Instruction log
                    local log = {}
                    log["time"] = currentTimestampInSeconds
                    log["consumer"] = consumerName
                    log["state"] = "Timeout"
                    log["stack"] = redis.call("DUMP", aIP)
                    redis.call("LPUSH", aCI, cjson.encode(log))

                    -- Delete previous instruction Pointer
                    redis.call("DEL", aIP)

                    -- Populate Instruction Pointer & Pop backlog
                    redis.call("HSET", aIP, "start", stepPeek["start"], "consumer", stepPeek["consumer"],
                        "argsFetchCompleted", stepPeek["argsFetchCompleted"], "consumers",
                        cjson.encode(stepPeek["consumers"]), "async", stepPeek["async"], "maximumTime",
                        stepPeek["maximumTime"], "opCode", stepPeek["opCode"], "stepName", stepPeek["stepName"], "args",
                        cjson.encode(stepPeek["args"])) -- Populate Instruction Pointer

                    return returnArray

                else
                    if (stepPeek["argsFetchCompleted"] == 1 and (currentTimestampInSeconds - stepPeek["start"]) <
                        stepPeek["maximumTime"] and stepPeek["async"] == 0) then
                        -- Async state
                        -- Populate Completed Instruction log
                        local log = {}
                        log["time"] = currentTimestampInSeconds
                        log["consumer"] = consumerName
                        log["state"] = "ASYNC"
                        log["stack"] = redis.call("DUMP", aIP)
                        redis.call("HSET", aAI, stepPeek["stepName"], cjson.encode(log))

                        -- Delete previous instruction Pointer
                        redis.call("DEL", aIP)
                        table.insert(returnArray, -1)
                        return returnArray
                    else
                        -- Step is currently in execution normally
                    end
                end
            end
        else
            -- This is when there are no more steps/instructions for this consumer
            table.insert(returnArray, -100)
            return returnArray
        end
    end
else
    -- Next or start state
    local stepPeek = redis.call("LRANGE", aIS, -1, -1)
    if (#stepPeek <= 0) then
        stepPeek = nil
        -- This is when there are no more instructions/steps in the algo
    else
        -- Instructions found
        stepPeek = cjson.decode(stepPeek[1]) -- Stringified IRedisStep Interface
        if (contains(stepPeek["consumers"], consumerName) or #stepPeek["consumers"] == 0) then
            -- Fetch Arguments
            local argsFetchCompleted = 1
            local argsFetched = {};
            for index, arg in pairs(stepPeek["args"]) do
                if (arg["type"] == "Reference") then
                    local paramValue = redis.call("HGET", aParams, arg["value"])
                    if (type(paramValue) == "boolean") then
                        argsFetchCompleted = 0
                    else
                        argsFetchCompleted = 1
                        argsFetched[arg["value"]] = paramValue
                    end
                end
            end

            -- Populate step if all args are found
            if (argsFetchCompleted == 1) then
                for index, arg in pairs(stepPeek["args"]) do
                    if (arg["type"] == "Reference") then
                        arg["value"] = argsFetched[arg["value"]]
                        arg["type"] = "Resolved"
                    end
                end
                table.insert(returnArray, 1)
                table.insert(returnArray, cjson.encode(stepPeek))
            else
                table.insert(returnArray, 0)
            end

            -- Populate Completed Instruction log
            local log = {}
            log["time"] = currentTimestampInSeconds
            log["consumer"] = consumerName
            log["state"] = "Next"
            redis.call("LPUSH", aCI, cjson.encode(log))

            -- Populate Instruction Pointer & Pop backlog
            redis.call("HSET", aIP, "start", currentTimestampInSeconds, "consumer", consumerName, "argsFetchCompleted",
                argsFetchCompleted, "consumers", cjson.encode(stepPeek["consumers"]), "async", stepPeek["async"],
                "maximumTime", stepPeek["maximumTime"], "opCode", stepPeek["opCode"], "stepName", stepPeek["stepName"],
                "args", cjson.encode(stepPeek["args"])) -- Populate Instruction Pointer
            redis.call("RPOP", aIS, 1) -- Popped from instructuion Backlog.

            return returnArray
        else
            -- This is when there are no more steps/instructions for this consumer
            table.insert(returnArray, -100)
            return returnArray
        end
    end
end

return returnArray

-- To test:
-- redis-cli --eval fetch.lua IS IP CI AI Params , C1

-- IRedisStep
-- {
--     "consumers": [],
--     "async": 0,
--     "maximumTime": -1,
--     "opCode": "NOP",
--     "algorithmName": "RedisTest",
--     "stepName": "Start",
--     "args": [
--       {
--         "type": "Literal",
--         "value": "Hello"
--       }
--     ]
--   }
