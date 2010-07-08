--[[---------------
Noki v0.4
-------------------
Noki is a toolkit to convert Nokia PC Suite backuped SMS to a
unicode .txt file, which is more accessible than the original
.nfb or .nfc.

It works well for Nokia PC Suite 6.5.12 and my mobile phone is 
Nokia 7360. There might be some compatibility problem if you
use earlier version of the PC Suite.

How to use:
 noki.save_sms('nokia.nfb', 'sms.txt')

Under the MIT license.

Noki is a part of LuaBit(http://luaforge.net/projects/bit/).

copyright(c) 2006~2007 hanzhao (abrash_han@hotmail.com)
--]]---------------

require 'hex'
require 'bit'

do
-- globals
local RETURN = '\13\0\10\0'
local SMS_FILE = '\\MPAPI\\MESSAGES'
local SMS_INBOX = 'PIT_MESSAGE_INBOX'
local SMS_OUTBOX = 'PIT_MESSAGE_OUTBOX'
local SMS_ARCHIVEBOX = 'PIT_MESSAGE_ARCHIVE'

-- output decorator
local SMS_INBOX_DEC = '[INBOX] '
local SMS_OUTBOX_DEC = '[OUTBOX] '
local SMS_ARCHIVE_DEC = '[ARCHIVE] '

-- box type
local BoxType = {
 NON = 0,
 IN = 1,
 OUT = 2,
 ARCHIVE = 3,
}

-- feed each char with an extra \0
local function asci_to_uni(asci)
 --print("-------")
 local uni = ""
 for i = 1, string.len(asci) do
  local str = string.format('%c\0', string.byte(asci, i))
  --print(string.len(str))
  uni = uni .. str

 end
 return uni
end

local function asci_padding(asci, pad)
 local uni = ""
 for i = 1, string.len(asci) do
  local str = string.format('%c', string.byte(asci, i))
  --print(string.len(str))
  uni = uni .. str .. pad

 end
 return uni
end

-- shrink the \0 in uni code string
local function uni_to_asci(uni)
 local asci = ''
 --print('uni len ' .. string.len(uni))
 for i = 1, string.len(uni), 2 do
  asci = asci .. string.sub(uni, i, i)
 end
 return asci
end

local function reader(str)
 local index = 1
 return function (n)
         --print('reader>>> idx ' .. index .. " n " .. n)
         local sub = string.sub(str, index, index + n - 1)
         --[[print(hex.to_hex(string.byte(sub, 1)))
         print(hex.to_hex(string.byte(sub, 2)))
         print(hex.to_hex(string.byte(sub, 3)))
         print(hex.to_hex(string.byte(sub, 4)))
         --]]
         index = index + n
         return sub
        end
end

local function read_number(read, n)
 local str = read(n)
 local rslt = 0
 for i = 1, n do
  local v = string.byte(str, i)
  rslt = bit.bor(rslt, bit.blshift(v, (i-1)*8))
 end
 return rslt
end

local function read_int(read)
 return read_number(read, 4)
end


local function read_short(read)
 return read_number(read, 2)
end

local function read_nfb_string(read)
 local len = read_int(read)
 local unistr = read(len*2)
 return unistr
end

local function read_nfb_header(read)
 local nfb_header = {
  ver = read_int(read),
  firmware = read_nfb_string(read),
  phone = read_nfb_string(read),
  entries = read_int(read),
 }
 return nfb_header
end

local function read_nfb_file(read)
 local nfbf = {}
 nfbf.path = read_nfb_string(read)

 nfbf.nbytes = read_int(read)

 nfbf.bytes = read(nfbf.nbytes)
 local stamp = read_int(read)

 return nfbf
end

local function read_nfb_dir(read)
 local nfbd = {
  path = read_nfb_string(read)
 }
 return nfbd
end

local function save_entry(fp, tbl)
 for k, v in pairs(tbl) do
  fp:write(v)
  fp:write(RETURN)
 end
end

-- save sms entries
local function save_sms(fp, ctnt)
 -- print("save sms ----")
 local in_box = asci_padding(SMS_INBOX, "%z")
 local out_box = asci_padding(SMS_OUTBOX, "%z")
 local archive_box = asci_padding(SMS_ARCHIVEBOX, "%z")
 local line_s = asci_padding("1020", "%z")
 local head = asci_padding("1033", "%z")
 local tail = asci_padding("1040", "%z")
 local service_center_tail = asci_padding("1080", "%z")
 local phone_nb = "%+%z%d%z%d%z[%d%z]+" -- default is type 145 with '+'
 local phone_nb_129 = string.rep("%d%z", 11) -- phone number type 129 without '+'
 local time = "[%d%z]+%-%z%d%z%d%z%-%z%d%z%d%zT%z%d%z%d%z:%z%d%z%d%z"

 local pattern = "([^\10]+)\13%z\10%z"
 local line_end = "\13%z\10%z"
 local lineb, linee = string.find(ctnt, line_end)
 local start = 1
 local line_number = 1
 while(lineb and linee) do
  local line = string.sub(ctnt, start, lineb - 1)
  --line = string.sub(ctnt, gb, ge)
  local type = BoxType.NON
  --print('capture ' .. string.len(line))
  --print(uni_to_asci(box))
  if(string.find(line, in_box)) then
   fp:write(asci_to_uni(SMS_INBOX_DEC))
   type = BoxType.IN
  elseif(string.find(line, out_box)) then
   fp:write(asci_to_uni(SMS_OUTBOX_DEC))
   type = BoxType.OUT
  elseif(string.find(line, archive_box)) then
   fp:write(asci_to_uni(SMS_ARCHIVE_DEC))
   type = BoxType.ARCHIVE
  else
   --print(uni_to_asci(line))
   io.close(fp)
   --error('unknown sms type')

   return
  end

   hb, he = string.find(line, head)
   tb, te = string.find(line, tail)

   local first_number = ""
   -- service center address
   sb, se = string.find(line, phone_nb, tb)
   --print("" .. sb .. ", " .. se)
   if(sb and se) then
    --print(uni_to_asci(string.sub(line, sb, se)))
    -- keep the find number, if the second find for sender address fails
    -- then this number is the sender address
    first_number = string.sub(line, sb, se)
   else
    sb, se = string.find(line, phone_nb_129, tb)
    if(not (sb and se)) then
	  --io.close(fp)
	  --error("error service center address")
	  --return
	  first_number = "empty number"
	 -- nokia's pc suite may leave the serivce center address empty
    end
   end
   
   -- sender address
   se_old = se
   sb, se = string.find(line, phone_nb, se)
   --print("" .. sb .. ", " .. se)
   local sender_address = ""
   if(sb and se) then
    --print(uni_to_asci(string.sub(line, sb, se)))
    sender_address = string.sub(line, sb, se)
   else
    sb, se = string.find(line, phone_nb_129, se_old)
    if(not (sb and se)) then
	 --[[
     print(line_number)
	 io.close(fp)
	 error("error sender address")
	 --]]
	 sender_address = first_number
    end
   end
   -- write sender
   fp:write(sender_address)
   fp:write(" \0")

   -- date time
   -- out box have no date time slot
   if(type ~= BoxType.OUT and first_number ~= "empty number") then
    tmb, tme = string.find(line, time, se)
    --print('' .. tmb .. ", " .. tme)
    if(tmb and tme) then
     --print(uni_to_asci(string.sub(line, tmb+1, tme)))
	else
	 io.close(fp)
	 error("error reading date time")
	 return
    end
    fp:write(string.sub(line, tmb+1, tme))
   end
   fp:write(RETURN)

   fp:write(string.sub(line, he+3, tb-3))
   
   fp:write(RETURN)
   fp:write(RETURN)
  --end
  start = linee + 1
  lineb, linee = string.find(ctnt, line_end, linee)
  line_number = line_number + 1
 end
end

-- save sms from a .nfc or .nfb file to a unicode .txt file
local function save_nfx_to(from, too)
	local fp = io.open(too, 'wb')
	if(not fp) then
	 error("error opening file " .. too .. " to write")
	 return
	end
	v = string.format('%c%c', 255, 254)
	-- unicode .txt 'FF FE'
	fp:write(v)

	-- read the .nfc file
	local nokia = io.open(from, 'rb')
	if(not nokia) then
	 error("error open file " .. from .. " to read")
	end
	local ctnt = nokia:read("*a")
	io.close(nokia)

	local read = reader(ctnt)

	local header = read_nfb_header(read)
	--print(header.ver)
	--print(header.entries)

	for i=1, header.entries do
	 --print(i)
	 local type = read_int(read)
	 if(type == 1) then
	  -- file entry
	  --print('file')
	  local fe = read_nfb_file(read)
	  --save_entry(fp, fe)
	  if(uni_to_asci(fe.path) == SMS_FILE) then
	   local smsctnt = fe.bytes
	   --print('sms len ' .. fe.nbytes)
	   save_sms(fp, smsctnt)
	   return
	  end

	 elseif(type == 2) then
	  -- dir entry
	  --print('dir')
	  local fd = read_nfb_dir(read)
	  --save_entry(fp, fd)
	 else
	  io.close(fp)
	  error('unknown entry type : ' .. hex.to_hex(type))
	 end
	end
	io.close(fp)
end

-- noki interface --
noki = {
 save_sms = save_nfx_to
}

end -- end block

-- sample
-- noki.save_sms('nokia2.nfb', 'sms2.txt')