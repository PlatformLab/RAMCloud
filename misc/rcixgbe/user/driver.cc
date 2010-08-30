#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <fcntl.h>
#include <inttypes.h>
#include <unistd.h>

#include <sys/mman.h>

#include "ixgbe_types.h"

#define GIGABYTE	(1024ULL * 1024 * 1024)

#define GIG_PAGE(_x)	((_x) >> 30)
#define GIG_OFFSET(_x)	((_x) & 0x3fffffff)

#define true 1
#define false 0

static uint8_t *nic_base;
static uint8_t *mem_base;
static uint64_t mem_len = GIGABYTE * 1;

static inline uint32_t
ixgbe_read_reg(uint32_t reg)
{
	volatile uint32_t *regp = (uint32_t *)(nic_base + reg);
	return *regp;
}

static inline void
ixgbe_write_reg(uint32_t reg, uint32_t val)
{
	volatile uint32_t *regp = (uint32_t *)(nic_base + reg);
	*regp = val;
}

// XXX- will need much fixing up
static uint64_t pabase;

static void
setup_addr_translations(void *base, size_t len)
{
	int fd = open("/proc/hugetlbphys", O_RDONLY); 
	if (fd == -1) {
		perror("open(hugetlbphys)");
		exit(1);
	}

	fprintf(stderr, "va:pa translations:\n");

	uint64_t off;
	for (off = 0; off < len; off += GIGABYTE) {
		uint64_t b = (uint64_t)base + off;
		uint64_t pa;

		// fault the mapping in
		volatile uint8_t *p = (uint8_t *)b;
		*p;

		int ret = pread(fd, &pa, sizeof(pa), b); 
		if (ret != sizeof(pa)) {
			perror("pread(hugetlbphys)");
			exit(1);
		}

		fprintf(stderr, "  0x%016lx : 0x%016lx\n", b, pa);
pabase = pa;
	}

	close(fd);
}

static void * 
va2pa(void *va)
{
	uint64_t number = GIG_PAGE((uint64_t)va);
	uint64_t offset = GIG_OFFSET((uint64_t)va);
	return (void *)(pabase + offset);
}

static void *
mmap_file(const char *path, size_t len, bool hugetlb)
{
	int flags = 0;

	if (hugetlb) {
		unlink(path);
		flags |= O_CREAT | O_TRUNC;
	}

	int fd = open(path, O_RDWR | flags);
	if (fd == -1) {
		fprintf(stderr, "open:");
		perror(path);
		exit(1);
	}

	void *hw_base = mmap(NULL, len,
	    PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (hw_base == MAP_FAILED) {
		fprintf(stderr, "mmap:");
		perror(path);
		exit(1);
	}

	return hw_base;
}

static void
setup_tx_ring(uint8_t *descbase, uint32_t descbytes)
{
	memset(descbase, 0, descbytes);

	// disable TX queue 0
	uint32_t dmatxctl = ixgbe_read_reg(IXGBE_TXDCTL(0));
	ixgbe_write_reg(IXGBE_TXDCTL(0), dmatxctl & ~IXGBE_TXDCTL_ENABLE);

	// set up the new descriptor base & length
	ixgbe_write_reg(IXGBE_TDBAL(0), (uint64_t)va2pa(descbase) & 0xffffffff);
	ixgbe_write_reg(IXGBE_TDBAH(0), (uint64_t)va2pa(descbase) >> 32);
	ixgbe_write_reg(IXGBE_TDLEN(0), descbytes);

	// reset head and tail
	ixgbe_write_reg(IXGBE_TDH(0), 0);
	ixgbe_write_reg(IXGBE_TDT(0), 0);

	// re-enable TX queue 0
	ixgbe_write_reg(IXGBE_TXDCTL(0), dmatxctl | IXGBE_TXDCTL_ENABLE);
}

static void
setup_rx_ring(uint8_t *descbase, uint32_t descbytes)
{
	memset(descbase, 0, descbytes);

	// disable RX queue 0
	uint32_t dmarxctl = ixgbe_read_reg(IXGBE_RXDCTL(0));
	ixgbe_write_reg(IXGBE_RXDCTL(0), dmarxctl & ~IXGBE_RXDCTL_ENABLE);

	// set up the new descriptor base & length
	ixgbe_write_reg(IXGBE_RDBAL(0), (uint64_t)va2pa(descbase) & 0xffffffff);
	ixgbe_write_reg(IXGBE_RDBAH(0), (uint64_t)va2pa(descbase) >> 32);
	ixgbe_write_reg(IXGBE_RDLEN(0), descbytes);

	// reset head and tail
	ixgbe_write_reg(IXGBE_RDH(0), 0);
	ixgbe_write_reg(IXGBE_RDT(0), 0);

	// re-enable RX queue 0
	ixgbe_write_reg(IXGBE_RXDCTL(0), dmarxctl | IXGBE_RXDCTL_ENABLE);
}

static int sendsize = 16;

static int16_t pktcnt = 0;

static void
send_pkt(const char *dst, const char *src)
{
	static int tx_i = 0;

	uint8_t *buf = mem_base + (1024 * 1024) + (tx_i * 2048);

        memcpy(buf,     dst, 6);
        memcpy(buf + 6, src, 6);
        memcpy(buf + 12, "\xaa\x55", 2);
        memcpy(buf + 14, &pktcnt, 2);

	int bufsize = sendsize;

	// kick out the frame
	u32 cmd_type_len = IXGBE_ADVTXD_DTYP_DATA |
			   IXGBE_ADVTXD_DCMD_DEXT |
			   IXGBE_ADVTXD_DCMD_RS   |
			   IXGBE_ADVTXD_DCMD_IFCS |
			   IXGBE_ADVTXD_DCMD_EOP;
	volatile union ixgbe_adv_tx_desc *tx_desc = (volatile union ixgbe_adv_tx_desc *)(mem_base + (tx_i * 16));
	tx_desc->read.buffer_addr   = (uint64_t)va2pa(buf);
	tx_desc->read.cmd_type_len  = cmd_type_len | bufsize;
	tx_desc->read.olinfo_status = bufsize << IXGBE_ADVTXD_PAYLEN_SHIFT;

	tx_i++;
	ixgbe_write_reg(IXGBE_TDT(0), tx_i);
}

static void
recv_pkt()
{
	static int rx_i = 0;

	uint8_t *buf = mem_base + (100 * 1024 * 1024) + (rx_i * 2048);

	volatile union ixgbe_adv_rx_desc *rx_desc = (volatile union ixgbe_adv_rx_desc *)(mem_base + 32768 * 16 + (rx_i * 16));
	rx_desc->read.pkt_addr = (uint64_t)va2pa(buf);
	rx_desc->read.hdr_addr = (uint64_t)va2pa(buf);

	rx_i++;
	ixgbe_write_reg(IXGBE_RDT(0), rx_i);

	// wait until transmission is complete
	while ((rx_desc->wb.upper.status_error & IXGBE_RXDADV_STAT_DD) == 0);

	memcpy(&pktcnt, buf + 14, 2);
	pktcnt++;
}

static void
get_mac_address(const char *path, char *buf)
{
	int fd = open(path, O_RDONLY);
	if (fd == -1) {
		perror(__func__);
		exit(1);
	}

	if (read(fd, buf, 6) != 6) {
		perror(__func__);
		exit(1);
	}

	close(fd);
}

static uint64_t
rdtsc()
{
	uint32_t lo, hi;
	__asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
	return (((uint64_t)hi << 32) | lo);
}

int
main(int argc, char **argv)
{
	bool sender = false;

	if (argc != 4 && argc != 5) {
		fprintf(stderr, "usage: %s ixgbe_device_file hugetlbfs_file sendsize\n",
		    argv[0]);
		exit(1);
	}

	if (argc == 5)
		sender = true;	

	nic_base = (uint8_t *)mmap_file(argv[1], 524288, false);
	fprintf(stderr, "nic regs @ va %p\n", nic_base);

	mem_base = (uint8_t *)mmap_file(argv[2], mem_len, true);
	fprintf(stderr, "memory @ va %p\n", mem_base);
	setup_addr_translations(mem_base, mem_len);

	setup_tx_ring(mem_base, 32768 * 16);	// 1024 16-byte descriptors
	fprintf(stderr, "tx queue re-enabled\n");

	setup_rx_ring(mem_base + 32768 * 16, 1024 * 16);
	fprintf(stderr, "rx queue re-enabled\n");

	sendsize = atoi(argv[3]);

	// get my mac address
	char src[6];
	get_mac_address(argv[1], src);
	fprintf(stderr, "mac address: %02x:%02x:%02x:%02x:%02x:%02x\n",
	    src[0], src[1], src[2], src[3], src[4], src[5]);

	uint64_t before = rdtsc();
	int i;
	int ncrap = 0;	// see if sending more pkts kicks dma engine faster
			// --- doesn't appear to!

	for (i = 0; i < 768; i++) {
		if (sender) {
			send_pkt("\xff\xff\xff\xff\xff\xff", src);
			for (int j = 0; j < ncrap; j++)
				send_pkt("\aa\55\aa\55\aa\55\aa", src);
		}
		recv_pkt();
		if (!sender) {
			send_pkt("\xff\xff\xff\xff\xff\xff", src);
			for (int j = 0; j < ncrap; j++)
				send_pkt("\aa\55\aa\55\aa\55\aa", src);
		}
	}
	uint64_t after = rdtsc();
	fprintf(stderr, "pktcnt: %d\n", pktcnt);
	fprintf(stderr, "total %lu ticks, avg rtt: %lu ticks\n", after - before, (after - before) / i);

	return 0;
}
