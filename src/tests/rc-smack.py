import ramcloud, random, time, sys

class rpcperf():
    def __init__(self):
        self.before_time = None
        self.total_time = 0
        self.last_report_time = time.time()
        self.befores = 0
        self.afters = 0
        self.rpcs = 0

    def __del__(self):
        print ""

    def before(self):
        self.before_time = time.time()
        self.befores += 1

    def after(self):
        assert self.before_time != None
        assert self.befores == (self.afters + 1)
        self.rpcs += 1
        ntime = time.time()
        diff = ntime - self.before_time
        self.total_time += diff

        if (ntime - self.last_report_time) >= 1.0:
            print "%60s" % ("") ,
            print "\r%.2f RPCs/sec (%.2f usec/RPC)" % (self.rpcs / self.total_time, 1.0e6 / (self.rpcs / self.total_time)) ,
            sys.stdout.flush()
            self.rpcs = 0
            self.total_time = 0
            self.last_report_time = ntime

        self.before_time = None
        self.afters += 1


# get a buffer of random printable characters
def getrandbuf(len):
    i = 0
    randbuf = ""
    while i < len:
        randbuf += chr(random.randint(32, 126))
        i += 1
    return randbuf


# try to ensure that conditional reads/write work
# properly
def version_smack(c, loops):
    randbuf = getrandbuf(2000)

    p = rpcperf()
    i = 0
    while i < loops:
        buf = randbuf[random.randint(0, 2000):]
        buf = buf[0:1000]

        p.before()
        last_version = c.write(0, 0, buf)
        p.after()

        caught = False
        p.before()
        try:
            try_version = last_version + 1
            if i & 0x1:
                try_version = last_version - 1
            c.write(0, 0, "Will you break for me?", try_version)
        except:
            caught = True
        p.after()
        assert caught

        p.before()
        rbuf, vers, indexes = c.read(0, 0)
        p.after()
        assert vers == last_version
        assert rbuf == buf

        caught = False
        p.before()
        try:
            try_version = last_version + 1;
            if i & 0x1:
                try_version = last_version - 1;
            rbuf, vers, indexes = c.read(0, 0, try_version)
        except:
            caught = True
        p.after()
        assert caught
        #XXX- exception should contain newest version... assert vers == last_version

        caught = False
        p.before()
        try:
            try_version = last_version + 1
            if i & 0x1:
                try_version = last_version - 1
            c.delete(0, 0, try_version)
        except:
            caught = True
        p.after()
        assert caught
        #XXX- exception should contain newest version... assert vers == last_version

        p.before()
        deleted_version = c.delete(0, 0, last_version)
        p.after()
        assert deleted_version == last_version

        caught = False
        p.before()
        try:
            c.delete(0, 0)
        except:
            caught = True 
        p.after()
        assert caught

        i += 1
        

# re-write random data to the same key over and over,
# making sure to re-read each time and confirm equality
def rewrite_smack(c, loops):
    randbuf = getrandbuf(2000)

    p = rpcperf()
    i = 0
    while i < loops:
        buf = randbuf[random.randint(0, 2000):]
        buf = buf[0:1000]

        p.before()
        c.write(0, 0, buf)
        p.after()

        p.before()
        rbuf, vers, indexes = c.read(0, 0)
        p.after()
        assert rbuf == buf
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

        p.before()
        key = c.insert(0, buf)
        p.after()

        p.before()
        rbuf, vers, indexes = c.read(0, key)
        p.after()
        assert rbuf == buf

        p.before()
        c.delete(0, key)
        p.after()
        i += 1


# write a value, then either re-write a new one over it, or delete
# first and then write out the new one (`p' probability of each case)
def rewrite_delete_smack(c, loops, p):
    randbuf = getrandbuf(2000)

    p = rpcperf()

    p.before()
    c.write(0, 0, randbuf[0:1000])
    p.after()

    i = 0
    while i < loops:
        buf = randbuf[random.randint(0, 2000):]
        buf = buf[0:1000]
        
        if random.random() < p:
            p.before()
            c.write(0, 0, buf)
            p.after()
        else:
            p.before()
            c.delete(0, 0)
            p.after()

            p.before()
            c.write(0, 0, buf)
            p.after()

        p.before()
        rbuf, vers, indexes = c.read(0, 0)
        p.after()
        assert rbuf == buf

        i += 1

smacks = 1000000
if len(sys.argv) == 2:
    smacks = int(sys.argv[1])
print "Using %d iterations/test" % (smacks)

c = ramcloud.RAMCloud()
c.connect()

print "Running version smack"
version_smack(c, smacks)

print "Running rewrite smack"
rewrite_smack(c, smacks)

print "Running delete smack"
delete_smack(c, smacks)

print "Running random rewrite/delete smack p = 0.3"
rewrite_delete_smack(c, smacks, 0.3)

print "Running random rewrite/delete smack p = 0.5"
rewrite_delete_smack(c, smacks, 0.5)

print "Running random rewrite/delete smack p = 0.8"
rewrite_delete_smack(c, smacks, 0.8)
