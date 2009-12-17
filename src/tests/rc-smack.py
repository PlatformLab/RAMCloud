import ramcloud, random

# get a buffer of random printable characters
def getrandbuf(len):
	i = 0
	randbuf = ""
	while i < len:
		randbuf += chr(random.randint(32, 126))
		i += 1
	return randbuf


# re-write random data to the same key over and over,
# making sure to re-read each time and confirm equality
def rewrite_smack(c):
	randbuf = getrandbuf(2000)

	i = 0
	while i < 1000000:
		buf = randbuf[random.randint(0, 2000):]
		buf = buf[0:1000]
		c.write(0, 0, buf)
		if c.read(0, 0) != buf:
			print "HOSED"
			break
		i += 1

# insert a value into the table, confirm re-read, and
# immediately delete it over and over. should exercise
# the tombstone code well
def delete_smack(c):
	randbuf = getrandbuf(2000)

	i = 0
	while i < 1000000:
		buf = randbuf[random.randint(0, 2000):]
		buf = buf[0:1000]
		key = c.insert(0, buf)
		if c.read(0, key) != buf:
			print "HOSED"
			break
		c.delete(0, key)
		i += 1

# write a value, then either re-write a new one over it, or delete
# first and then write out the new one (`p' probability of each case)
def rewrite_delete_smack(c, p):
	randbuf = getrandbuf(2000)

	c.write(0, 0, randbuf[0:1000])

	i = 0
	while i < 10000000:
		buf = randbuf[random.randint(0, 2000):]
		buf = buf[0:1000]
		
		if random.random() < p:
			c.write(0, 0, buf)
		else:
			c.delete(0, 0)
			c.write(0, 0, buf)

		if c.read(0, 0) != buf:
			print "HOSED"
			break 

c = ramcloud.RAMCloud()
c.connect()

print "Running rewrite smack"
rewrite_smack(c)

print "Running delete smack"
delete_smack(c)

print "Running random rewrite/delete smack p = 0.3"
rewrite_delete_smack(c, 0.3)

print "Running random rewrite/delete smack p = 0.5"
rewrite_delete_smack(c, 0.5)

print "Running random rewrite/delete smack p = 0.8"
rewrite_delete_smack(c, 0.8)
