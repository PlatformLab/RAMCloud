import ramcloud, random, time, sys

class rpcperf():
    def __init__(self):
        self.time = time.time()
        self.rpcs = 0

    def __del__(self):
        print >> sys.stderr, ""

    def update(self):
        self.rpcs += 1
        ntime = time.time()
        diff = ntime - self.time
        if diff >= 1.0:
            print >> sys.stderr, "%80s" % ("") ,
            print >> sys.stderr, "\r%.2f RPCs/sec (%.2f usec/RPC)" % (self.rpcs / diff, 1.0e6 / (self.rpcs / diff)) ,
            self.rpcs = 0
            self.time = ntime


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
def rewrite_smack(c, loops):
    randbuf = getrandbuf(2000)

    p = rpcperf()
    i = 0
    while i < loops:
        buf = randbuf[random.randint(0, 2000):]
        buf = buf[0:1000]
        c.write(0, 0, buf)
        p.update()
        if c.read(0, 0) != buf:
            print "HOSED"
            break
        p.update()
        i += 1


# insert a value into the table, confirm re-read, and
# immediately delete it over and over. should exercise
# the tombstone code well
def delete_smack(c, loops):
    randbuf = getrandbuf(2000)

    p = rpcperf()
    i = 0
    while i < loops:
        buf = randbuf[random.randint(0, 2000):]
        buf = buf[0:1000]
        key = c.insert(0, buf)
        p.update()
        if c.read(0, key) != buf:
            print "HOSED"
            break
        p.update()
        c.delete(0, key)
        p.update()
        i += 1


# write a value, then either re-write a new one over it, or delete
# first and then write out the new one (`p' probability of each case)
def rewrite_delete_smack(c, loops, p):
    randbuf = getrandbuf(2000)

    p = rpcperf()

    c.write(0, 0, randbuf[0:1000])
    p.update()

    i = 0
    while i < loops:
        buf = randbuf[random.randint(0, 2000):]
        buf = buf[0:1000]
        
        if random.random() < p:
            c.write(0, 0, buf)
            p.update()
        else:
            c.delete(0, 0)
            p.update()
            c.write(0, 0, buf)
            p.update()

        if c.read(0, 0) != buf:
            print "HOSED"
            break 
        p.update()


c = ramcloud.RAMCloud()
c.connect()

print "Running rewrite smack"
rewrite_smack(c, 1000000)

print "Running delete smack"
delete_smack(c, 1000000)

print "Running random rewrite/delete smack p = 0.3"
rewrite_delete_smack(c, 1000000, 0.3)

print "Running random rewrite/delete smack p = 0.5"
rewrite_delete_smack(c, 1000000, 0.5)

print "Running random rewrite/delete smack p = 0.8"
rewrite_delete_smack(c, 1000000, 0.8)
