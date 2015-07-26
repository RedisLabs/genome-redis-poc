--[[
hashistogram.lua

Assumes the database is made up of hashes and returns
a histogram of their sizes

KEYS[] = none
ARGV[] =
  #1 - number of SCAN iterations, 0 for all (default is 0)

]]--

local h = {}
local r = {}
local t = {}
local tk = 0
local n = tonumber(ARGV[1])
if n == nil then n = -1 end

local c = 0

repeat
  n = n - 1
  local t = redis.call('SCAN', c)
  c = tonumber(t[1])
  for k, v in pairs(t[2]) do
    local s = redis.call('HLEN', v)
    if h[s] == nil then
      h[s] = 1
    else
      h[s] = h[s] + 1
    end
    tk = tk + 1
  end
until c == 0 or n == 0

for k in pairs(h) do t[#t+1] = k end
table.sort(t)

for i = 1, #t do
  r[#r+1] = 'count: ' .. h[t[i]]  .. ' hlen: ' .. t[i]
end
r[#r+1] = 'total keys scanned: ' .. tk

return (r)
