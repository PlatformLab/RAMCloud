-- (c) 2010 Stanford University
-- Wireshark is under the GPL, so I think this script has to be as well.

print("hello world!")

require "luabit/bit"

----------------
-- from http://ricilake.blogspot.com/2007/10/iterating-bits-in-lua.html:
function getbit(p)
	return 2 ^ (p - 1) -- 1-based indexing
end

-- Typical call: if hasbit(x, getbit(3)) then ...
function hasbit(x, p)
	return x % (p + p) >= p
end
----------------

do
	local p_ack_proto = Proto("rcack", "RAMCloud ACK")
	local p_sess_proto = Proto("rcsess", "RAMCloud Session Open")
	local maxpt = 0

	do -- RAMCloud session open response dissector
		local f_maxChannelId = ProtoField.uint16("rcsess.maxChannelId", "Max Avail. Channel Id", base.DEC)
		p_sess_proto.fields = { f_maxChannelId }

		function p_sess_proto.dissector(buf, pkt, root)
			local t = root:add(p_sess_proto, buf(0))
			t:add_le(f_maxChannelId, buf(0, 1))
		end
	end

	do -- RAMCloud ACK response dissector
		local p_frag_proto = Proto("rcackgaps", "Fragments")
		local f_ack = ProtoField.uint16("rcack.ack", "ACK", base.DEC)
		p_frag_proto.fields = { f_ack }

		local f_firstMissingFrag = ProtoField.uint16("rcack.firstMissingFrag", "First Missing Fragment", base.DEC)
		p_ack_proto.fields = { f_firstMissingFrag }

		function p_ack_proto.dissector(buf, pkt, root)
			local t = root:add(p_ack_proto, buf(0))
			t:add_le(f_firstMissingFrag, buf(0, 2))

			local u = t:add(p_frag_proto, buf(2))

			local bitNumber = 0
			while bitNumber < 32 do
				local byte = buf(2 + bitNumber / 8, 1):le_uint()
				if bit.band(byte, bit.blshift(1, bitNumber % 8)) ~= 0 then
					u:add_le(f_ack, bitNumber + buf(0, 2):le_uint() + 1)
				end
				bitNumber = bitNumber + 1
			end

			-- Dissector.get("data"):call(buf(2):tvb(), pkt, t)
		end
	end
	do -- RAMCloud transport header dissector
		local p_proto = Proto("rc", "RAMCloud")

		local f_sessionToken = ProtoField.uint64("rc.sessionToken", "Session Token", base.HEX)
		local f_rpcId = ProtoField.uint32("rc.rpcId", "RPC ID", base.HEX)
		local f_clientSessionHint = ProtoField.uint32("rc.clientSessionHint", "Client Session Hint", base.DEC)
		local f_serverSessionHint = ProtoField.uint32("rc.serverSessionHint", "Server Session Hint", base.DEC)
		local f_fragNumber = ProtoField.uint16("rc.fragNumber", "Fragment Number", base.DEC)
		local f_totalFrags = ProtoField.uint16("rc.totalFrags", "Total Fragments", base.DEC)
		local f_channelId = ProtoField.uint8("rc.channelId", "Channel ID", base.DEC)
		local t_payloadTypes = { [0x0] = "DATA",
			[0x1] = "ACK",
			[0x2] = "SESSION_OPEN",
			[0x3] = "RESERVED 1",
			[0x4] = "BAD_SESSION",
			[0x5] = "RETRY_WITH_NEW_RPCID",
			[0x6] = "RESERVED 2",
			[0x7] = "RESERVED 3"}
		local f_payloadType = ProtoField.uint8("rc.payloadType", "Payload Type", base.DEC,
						       t_payloadTypes, 0xF0)
		local f_direction = ProtoField.uint8("rc.direction", "Direction", nil,
					       { [0] = "client to server", [1] = "server to client" }, 0x01)
		local f_requestAck = ProtoField.uint8("rc.requestAck", "Request ACK", nil, nil, 0x02)
		local f_pleaseDrop = ProtoField.uint8("rc.pleaseDrop", "Please Drop", nil, nil, 0x04)
		p_proto.fields = { f_sessionToken, f_rpcId, f_clientSessionHint, f_serverSessionHint,
		                   f_fragNumber, f_totalFrags,
		                   f_channelId, f_payloadType, f_direction, f_requestAck, f_pleaseDrop }
		
		function p_proto.dissector(buf, pkt, root)
			local header_len = 26
			local t = root:add(p_proto, buf(0, header_len))
			t:add_le(f_sessionToken, buf(0, 8))
			t:add_le(f_rpcId, buf(8, 4))
			t:add_le(f_clientSessionHint, buf(12, 4))
			t:add_le(f_serverSessionHint, buf(16, 4))
			t:add_le(f_fragNumber, buf(20, 2))
			t:add_le(f_totalFrags, buf(22, 2))
			t:add_le(f_channelId, buf(24, 1))
			t:add_le(f_payloadType, buf(25, 1))
			t:add_le(f_direction, buf(25, 1))
			t:add_le(f_requestAck, buf(25, 1))
			t:add_le(f_pleaseDrop, buf(25, 1))

			local i_payloadType = bit.blogic_rshift(buf(25, 1):le_uint(), 4)
			if i_payloadType > maxpt then
				maxpt = i_payloadType
				print(i_payloadType)
			end
			local i_direction = bit.band(buf(25, 1):le_uint(), 0x01)
			local i_requestAck = bit.band(buf(25, 1):le_uint(), 0x02)

			local s_fragDisplay = "";
			local s_directionDisplay = "";
			local s_requestAckDisplay = "";
			if i_payloadType == 0x0 then
				s_fragDisplay = "[" .. buf(20,2):le_uint() .. "/" .. buf(22,2):le_uint() - 1 .. "] "
				if i_requestAck ~= 0 then
					s_requestAckDisplay = " requesting ACK"
				end
			end
			if i_payloadType ~= 0x1 then
				if i_direction == 0 then
					s_directionDisplay = "request"
				else
					s_directionDisplay = "response"
				end
			else
				if i_direction == 0 then
					s_directionDisplay = "to server"
				else
					s_directionDisplay = "to client"
				end
			end

			local s_info = buf(0, 1):le_uint() .. "." .. buf(24, 1):le_uint() .. "." .. buf(8, 1):le_uint() .. ": "
			s_info = s_info .. s_fragDisplay .. t_payloadTypes[i_payloadType] .. " "
			s_info = s_info .. s_directionDisplay .. s_requestAckDisplay
			pkt.columns.info = s_info
			pkt.columns.protocol = "RAMCloud"

			if i_payloadType == 0x1 then
				p_ack_proto.dissector:call(buf(header_len):tvb(), pkt, root)
			elseif i_payloadType == 0x2 and i_direction == 1 then
				p_sess_proto.dissector:call(buf(header_len):tvb(), pkt, root)
			else
				Dissector.get("data"):call(buf(header_len):tvb(), pkt, root)
			end
		end

		local udp_encap_table = DissectorTable.get("udp.port")
		udp_encap_table:add(12242, p_proto)
        local eth_dissector_table = DissectorTable.get("ethertype")
        if eth_dissector_table ~= nil then
            eth_dissector_table:add(0x8001, p_proto)
        end
	end
end
