#include <assert.h>
#include <stdio.h>
#define _XOPEN_SOURCE 600	/* for posix_memalign */
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <infiniband/verbs.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>

struct exchange_params {
	int lid;
	int qpn;
};

static int remote_qpn;

#define QKEY 0xdeadbeef
#define DEPTH 1024
#define NUM_RTTS 1000000
#define TICKS_PER_USEC 2400

static uint64_t rtt_times[NUM_RTTS];

static uint64_t
rdtsc()
{
        uint32_t lo, hi;
        __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
        return (((uint64_t)hi << 32) | lo);
}

static int
ibGetLID(struct ibv_context *ctxt, int port)
{
	struct ibv_port_attr ipa;
	if (ibv_query_port(ctxt, port, &ipa)) {
		fprintf(stderr, "ibv_query_port failed\n");
		exit(1);
	}
	return ipa.lid;
}

static struct ibv_device *
ibFindDevice(const char *name)
{
	struct ibv_device **devices;

	devices = ibv_get_device_list(NULL);
	if (devices == NULL)
		return NULL;

	if (name == NULL)
		return devices[0];

	for (int i = 0; devices[i] != NULL; i++) {
		if (strcmp(devices[i]->name, name) == 0)
			return devices[i];
	}

	return NULL;
}

static void
ibPostReceive(struct ibv_qp *qp, struct ibv_mr *mr, char *rxbuf, size_t rxbufsize)
{
	struct ibv_sge isge = { (uint64_t)rxbuf, rxbufsize, mr->lkey };
	struct ibv_recv_wr irwr;

	memset(&irwr, 0, sizeof(irwr));
	irwr.wr_id = 1;
	irwr.next = NULL;
	irwr.sg_list = &isge;
	irwr.num_sge = 1;

	struct ibv_recv_wr *bad_irwr;
	if (ibv_post_recv(qp, &irwr, &bad_irwr)) {
		fprintf(stderr, "failed to ibv_post_recv\n");
		exit(1);
	}
}

static void
ibPostSend(struct ibv_ah *ah, struct ibv_qp *qp, struct ibv_mr *mr, char *txbuf, size_t txbufsize)
{
	struct ibv_sge isge = { (uint64_t)txbuf, txbufsize, mr->lkey };
	struct ibv_send_wr iswr;

	memset(&iswr, 0, sizeof(iswr));
	iswr.wr.ud.ah = ah;
	iswr.wr.ud.remote_qpn = remote_qpn; 
	iswr.wr.ud.remote_qkey = QKEY;
	iswr.wr_id = 2;
	iswr.next = NULL;
	iswr.sg_list = &isge;
	iswr.num_sge = 1;
	iswr.opcode = IBV_WR_SEND;
	iswr.send_flags = IBV_SEND_SIGNALED;

	struct ibv_send_wr *bad_iswr;
	if (ibv_post_send(qp, &iswr, &bad_iswr)) {
		fprintf(stderr, "ibv_post_send failed!\n");
		exit(1);
	}
}

static void
ibPostSendAndWait(struct ibv_ah *ah, struct ibv_qp *qp, struct ibv_mr *mr, char *txbuf, size_t txbufsize, struct ibv_cq *cq)
{
	ibPostSend(ah, qp, mr, txbuf, txbufsize);

	struct ibv_wc iwc;
	while (ibv_poll_cq(cq, 1, &iwc) < 1)
		;
	if (iwc.status != IBV_WC_SUCCESS) {
		fprintf(stderr, "ibv_poll_cq returned failure\n");
		exit(1);
	}
}

static void
server(struct ibv_ah *ah, struct ibv_qp *qp, struct ibv_mr *mr, struct ibv_cq *rxcq, char *rxbuf, size_t rxbufsize, struct ibv_cq *txcq, char *txbuf, size_t txbufsize)
{
	while (1) {
		// receive first
		struct ibv_wc iwc;
		while (ibv_poll_cq(rxcq, 1, &iwc) < 1)
			;
		if (iwc.status != IBV_WC_SUCCESS) {
			fprintf(stderr, "ibv_poll_cq returned failure\n");
			exit(1);
		}
		assert(iwc.byte_len == 44);

		// now respond
		int counter = ((volatile uint32_t *)rxbuf)[10] + 1;
		if (counter == 0)
			break;

		// re-add rx buf
		ibPostReceive(qp, mr, rxbuf, rxbufsize);

		memcpy(txbuf, &counter, sizeof(counter));
		ibPostSendAndWait(ah, qp, mr, txbuf, sizeof(counter), txcq);
	}

	printf("server exiting...\n");
}

static void
client(struct ibv_ah *ah, struct ibv_qp *qp, struct ibv_mr *mr, struct ibv_cq *rxcq, char *rxbuf, size_t rxbufsize, struct ibv_cq *txcq, char *txbuf, size_t txbufsize)
{
	int counter = 583742;

	memcpy(txbuf, &counter, sizeof(counter));

	uint64_t min_rtt = 0xffffffffffffffffUL, max_rtt = 0;

	uint64_t b = rdtsc();
	for (int i = 0; i < NUM_RTTS; i++) {
		uint64_t m_b = rdtsc();

		// send first
		ibPostSendAndWait(ah, qp, mr, txbuf, sizeof(counter), txcq);

		// now receive
		struct ibv_wc iwc;
		while (ibv_poll_cq(rxcq, 1, &iwc) < 1)
			;
		if (iwc.status != IBV_WC_SUCCESS) {
			fprintf(stderr, "ibv_poll_cq returned failure\n");
			exit(1);
		}
		assert(iwc.byte_len == 44);

		int newcount = ((volatile uint32_t *)rxbuf)[10];
		assert(counter + 1 == newcount);
		counter = newcount;

		// re-add rx buf
		ibPostReceive(qp, mr, rxbuf, rxbufsize);

		memcpy(txbuf, &counter, sizeof(counter));

		uint64_t m_a = rdtsc();
		if (m_a - m_b < min_rtt)
			min_rtt = m_a - m_b;
		if (m_a - m_b > max_rtt)
			max_rtt = m_a - m_b;

		rtt_times[i] = m_a - m_b;
	}
	uint64_t a = rdtsc();

	// communicate completion
	counter = 0xffffffff;
	memcpy(txbuf, &counter, sizeof(counter));
	ibPostSend(ah, qp, mr, txbuf, sizeof(counter));

	printf("client exiting...\n");
	printf("client did %d RTTs in %lu ticks (%lu ticks/RTT)\n", NUM_RTTS, a - b, (a - b) / NUM_RTTS); 
	printf("client min ticks/RTT: %lu, max ticks/RTT: %lu\n", min_rtt, max_rtt);
	printf("avg: %.3f usec\n", (float)(a - b) / (float)NUM_RTTS / (float)TICKS_PER_USEC);

	printf("histogram:\n");
	for (int i = 0; i < NUM_RTTS; i++)
		rtt_times[i] /= TICKS_PER_USEC;
	int total = 0;
	for (int i = min_rtt / TICKS_PER_USEC; i < max_rtt / TICKS_PER_USEC; i++) {
		int cnt = 0;
		for (int j = 0; j < NUM_RTTS; j++) {
			if (rtt_times[j] == i)
				cnt++;
		}
		total += cnt;
		if (cnt != 0)
			printf("  %-4d : %-10d (cdf %.4f%%)\n", i, cnt, (float)total * 100.0 / (float)NUM_RTTS);
	}
	
}

static struct exchange_params
client_exchange(const char *server, uint16_t port, struct exchange_params *params)
{
	int s = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (s == -1) {
		perror("socket");
		exit(1);
	}

	struct hostent *hent = gethostbyname(server);	
	if (hent == NULL) {
		perror("gethostbyname");
		exit(1);
	}

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof(sin));
	sin.sin_family = PF_INET;
	sin.sin_port = htons(port);
	sin.sin_addr = *((struct in_addr *)hent->h_addr);

	if (connect(s, (struct sockaddr *)&sin, sizeof(sin)) == -1) {
		perror("connect");
		exit(1);
	}

	write(s, params, sizeof(*params));
	read(s, params, sizeof(*params));

	close(s);

	return *params;
}

static struct exchange_params
server_exchange(uint16_t port, struct exchange_params *params)
{
	int s = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (s == -1) {
		perror("socket");
		exit(1);
	}

	int on = 1;
	if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) == -1) {
		perror("setsockopt");
		exit(1);
	}

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof(sin));
	sin.sin_family = PF_INET;
	sin.sin_port = htons(port);
	sin.sin_addr.s_addr = htonl(INADDR_ANY);

	if (bind(s, (struct sockaddr *)&sin, sizeof(sin)) == -1) {
		perror("bind");
		exit(1);
	}

	if (listen(s, 1) == -1) {
		perror("listen");
		exit(1);
	}

	struct sockaddr_in csin;
	socklen_t csinsize = sizeof(csin);
	int c = accept(s, (struct sockaddr *)&csin, &csinsize);
	if (c == -1) {
		perror("accept");
		exit(1);
	}

	write(c, params, sizeof(*params));
	read(c, params, sizeof(*params));

	close(c);
	close(s);

	return *params;
}

int
main(int argc, char **argv)
{
	struct ibv_device *dev = NULL;
	int tcp_port = 18515;
	int ib_port  = 1;
	const char *servername = argv[1];

	srand48(time(NULL) * getpid());

	dev = ibFindDevice(NULL);
	if (dev == NULL) {
		fprintf(stderr, "failed to find infiniband device\n");
		exit(1);
	}

	printf("Using ib device `%s'.\n", dev->name);

	struct ibv_context *ctxt = ibv_open_device(dev);
	if (ctxt == NULL) {
		fprintf(stderr, "failed to open infiniband device\n");
		exit(1);
	}

	// allocate a protection domain for our memory region
	struct ibv_pd *pd = ibv_alloc_pd(ctxt);
	if (pd == NULL) {
		fprintf(stderr, "failed to allocate infiniband pd\n");
		exit(1);
	}

	void *buf;
	const size_t bufsize = 8 * 1024 * 1024;
	if (posix_memalign(&buf, 4096, bufsize)) {
		fprintf(stderr, "posix_memalign failed\n");
		exit(1);
	}
	char *txbuf = (char *)buf; 
	size_t txbufsize = bufsize/2;
	char *rxbuf = &txbuf[bufsize/2];
	size_t rxbufsize = bufsize/2;

	// register our userspace buffer with the HCA
	struct ibv_mr *mr = ibv_reg_mr(pd, buf, bufsize,
	    IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);
	if (mr == NULL) {
		fprintf(stderr, "failed to register memory region\n");
		exit(1);
	}

	// create completion queues for receive and transmit
	struct ibv_cq *rxcq = ibv_create_cq(ctxt, DEPTH, NULL, NULL, 0);
	if (rxcq == NULL) {
		fprintf(stderr, "failed to create receive completion queue\n");
		exit(1);
	}
	struct ibv_cq *txcq = ibv_create_cq(ctxt, DEPTH, NULL, NULL, 0);
	if (txcq == NULL) {
		fprintf(stderr, "failed to create receive completion queue\n");
		exit(1);
	}

	// fill in a big struct of queue pair parameters
	struct ibv_qp_init_attr qpia;
	memset(&qpia, 0, sizeof(qpia));
	qpia.send_cq = txcq;
	qpia.recv_cq = rxcq;
	qpia.cap.max_send_wr  = DEPTH;	// max outstanding send requests
	qpia.cap.max_recv_wr  = DEPTH;	// max outstanding recv requests
	qpia.cap.max_send_sge = 1;	// max send scatter-gather elements
	qpia.cap.max_recv_sge = 1;	// max recv scatter-gather elements
	qpia.cap.max_inline_data = 0;	// max bytes of immediate data on send q
	qpia.qp_type = IBV_QPT_UD;	// RC, UC, UD, or XRC
	qpia.sq_sig_all = 0;		// only generate CQEs on requested WQEs

	// create the queue pair
	struct ibv_qp *qp = ibv_create_qp(pd, &qpia);
	if (qp == NULL) {
		fprintf(stderr, "failed to create queue pair\n");
		exit(1);
	}

	// move from RESET to INIT state
	struct ibv_qp_attr qpa;
	memset(&qpa, 0, sizeof(qpa));
	qpa.qp_state   = IBV_QPS_INIT;
	qpa.pkey_index = 0;
	qpa.port_num   = ib_port;
	qpa.qkey       = QKEY;		// docs don't say what this is
	if (ibv_modify_qp(qp, &qpa,  IBV_QP_STATE |
				     IBV_QP_PKEY_INDEX |
				     IBV_QP_PORT |
				     IBV_QP_QKEY)) {
		fprintf(stderr, "failed to modify qp state 1\n");
		exit(1);
	}

	// exchange qp info over TCP so we can move to the RTR and RTS states
	struct exchange_params params = { ibGetLID(ctxt, ib_port), qp->qp_num };
	printf("Local  LID 0x%x, QPN 0x%x\n", params.lid, params.qpn );
	if (servername)
		params = client_exchange(servername, tcp_port, &params);
	else
		params = server_exchange(tcp_port, &params);
	printf("Remote LID 0x%x, QPN 0x%x\n", params.lid, params.qpn);

	// add receive buf so we can --> RTR
	// XXX: we add the same buf multiple times. latency is _much_ better with
	//      more RX bufs.
	for (int j = 0; j < DEPTH; j++)
		ibPostReceive(qp, mr, rxbuf, rxbufsize);

	// now switch to RTR
	memset(&qpa, 0, sizeof(qpa));
	qpa.qp_state = IBV_QPS_RTR;

	if (ibv_modify_qp(qp, &qpa, IBV_QP_STATE)) {
		fprintf(stderr, "failed to modify qp state 2\n");
		exit(1);
	}

	// now move to RTS
	qpa.qp_state = IBV_QPS_RTS;
	qpa.sq_psn = lrand48() & 0xffffff;
	if (ibv_modify_qp(qp, &qpa, IBV_QP_STATE | IBV_QP_SQ_PSN)) {
		fprintf(stderr, "failed to modify qp state 3\n");
		exit(1);
	}

	struct ibv_ah_attr attr;
	attr.dlid = params.lid;
	attr.src_path_bits = 0;
	attr.is_global = 0;
	attr.sl = 0;
	attr.port_num = 1;
	
uint64_t b = rdtsc();
	struct ibv_ah *ah = ibv_create_ah(pd, &attr);
printf("ibv_create_ah took %lu cycles\n", rdtsc() - b);
	if (ah == NULL) {
		fprintf(stderr, "failed to create ah\n");
		exit(1);
	}


	// XXX- should handshake again over TCP socket to synchronise
	if (servername)
		sleep(1);	// have client sleep for server to poll on rx

	remote_qpn = params.qpn;	
	if (servername)
		client(ah, qp, mr, rxcq, rxbuf, rxbufsize, txcq, txbuf, txbufsize);
	else
		server(ah, qp, mr, rxcq, rxbuf, rxbufsize, txcq, txbuf, txbufsize);

	return 0;
}
