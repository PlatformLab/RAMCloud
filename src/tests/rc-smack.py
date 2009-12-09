import ramcloud, random

c = ramcloud.RAMCloud()
c.connect()

i = 0
randbuf = ""
while i < 2000:
	randbuf += chr(random.randint(32, 126))
	i += 1

i = 0
while i < 1000000:
	buf = randbuf[random.randint(0, 2000):]
	buf = buf[0:1000]
	c.write(0, 0, buf)
	if c.read(0, 0) != buf:
		print "HOSED"
		break
	i += 1
