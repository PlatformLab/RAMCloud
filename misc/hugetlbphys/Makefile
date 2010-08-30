obj-m := hugetlbphys.o

KERN = $(shell uname -r)
PWD  = $(shell pwd)

all:
	make -C /lib/modules/$(KERN)/build M=$(PWD) modules

clean:
	make -C /lib/modules/$(KERN)/build M=$(PWD) clean
