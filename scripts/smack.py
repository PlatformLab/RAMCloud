import ramcloud, random, time, sys
import optparse

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
        last_version = c.write(1, 0, buf)
        p.after()

        caught = False
        p.before()
        try:
            try_version = last_version + 1
            if i & 0x1:
                try_version = last_version - 1
            c.write(1, 0, "Will you break for me?", try_version)
        except:
            caught = True
        p.after()
        assert caught

        p.before()
        rbuf, vers = c.read(1, 0)
        p.after()
        assert vers == last_version
        assert rbuf == buf

        caught = False
        p.before()
        try:
            try_version = last_version + 1;
            if i & 0x1:
                try_version = last_version - 1;
            rbuf, vers = c.read(1, 0, try_version)
        except:
            caught = True
        p.after()
        assert caught
        #TODO(Rumble): exception should contain newest version... assert vers == last_version

        caught = False
        p.before()
        try:
            try_version = last_version + 1
            if i & 0x1:
                try_version = last_version - 1
            c.delete(1, 0, try_version)
        except:
            caught = True
        p.after()
        assert caught
        #TODO(Rumble): exception should contain newest version... assert vers == last_version

        p.before()
        c.delete(1, 0, last_version)
        p.after()

        caught = False
        p.before()
        try:
            c.delete(1, 0)
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
        c.write(1, 0, buf)
        p.after()

        p.before()
        rbuf, vers = c.read(1, 0)
        p.after()
        assert rbuf == buf
        i += 1


# write a value into the table, confirm re-read, and
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
        c.write(1, 0, buf)
        p.after()

        p.before()
        rbuf, vers = c.read(1, 0)
        p.after()
        assert rbuf == buf

        p.before()
        c.delete(1, 0)
        p.after()
        i += 1


# write a value, then either re-write a new one over it, or delete
# first and then write out the new one (`p' probability of each case)
def rewrite_delete_smack(c, loops, p):
    randbuf = getrandbuf(2000)

    p = rpcperf()

    p.before()
    c.write(1, 0, randbuf[0:1000])
    p.after()

    i = 0
    while i < loops:
        buf = randbuf[random.randint(0, 2000):]
        buf = buf[0:1000]
        
        if random.random() < p:
            p.before()
            c.write(1, 0, buf)
            p.after()
        else:
            p.before()
            c.delete(1, 0)
            p.after()

            p.before()
            c.write(1, 0, buf)
            p.after()

        p.before()
        rbuf, vers = c.read(1, 0)
        p.after()
        assert rbuf == buf

        i += 1

# Assumes 8MB segments, 10 Segments needed to clean
def cleaner_consistency_smack(c, smacks):
    if smacks < 10000:
        return

    p = rpcperf()
    buf10k = getrandbuf(10 * 1024);

    # write a bunch of data
    i = 0
    while i < 8192:
        p.before()
        c.write(1, i, str(i) + "." + buf10k)
        p.after()

        i += 1

    # delete to make cleanable
    i = 0
    while i < 8192:
        if (i % 3) != 0:
            p.before()
            c.delete(1, i)
            p.after()
        i += 1

    # write a bunch more to get the previous segments cleaned
    while i < 16384:
        p.before()
        c.write(1, i, str(i) + "." + buf10k)
        p.after()

        i += 1

    # ensure only the mod 3 objects with IDs < 8192 are around
    i = 0
    while i < 16384:
        p.before()
        isLive = True
        buf = ""
        try:
            buf = c.read(1, i)
        except:
            isLive = False
        p.after()

        if isLive:
            assert int(buf[0].split(".")[0]) == i

        if i < 8192 and (i % 3) != 0:
            assert not isLive
        else:
            assert isLive

        i += 1

def main():
    parser = optparse.OptionParser()
    parser.add_option('-n', '--number', dest='smacks', default=10000, type=int)
    parser.add_option('-C', '--coordinator',
                      dest='coordinatorLocator',
                      default='basic+udp:ip=127.0.0.1,port=12242',
                      type=str)
    (options, args) = parser.parse_args()

    smacks = options.smacks

    print 'Connecting to %s' % options.coordinatorLocator
    print "Using %d iterations/test" % smacks

    c = ramcloud.RAMCloud()
    c.connect(options.coordinatorLocator)
    c.create_table("test")

    print "Running cleaner consistency smack"
    cleaner_consistency_smack(c, smacks)

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

if __name__ == '__main__':
    main()
