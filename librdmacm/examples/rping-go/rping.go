package main

/*
#cgo LDFLAGS: -lrdmacm -libverbs
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>
#include <endian.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <netdb.h>
#include <sys/eventfd.h>
#include <sys/poll.h>
#include <unistd.h>
#include <stdio.h>

// rping_rdma_info structure matching the C definition
struct rping_rdma_info {
	uint64_t buf;   // stored in network byte order
	uint32_t rkey;  // stored in network byte order
	uint32_t size;  // stored in network byte order
};

// --- All-in-one C-side control block to avoid cgo Go pointer rules ---
// This struct holds all buffers and work requests on the C heap,
// so we never pass Go pointers to C functions.

struct rping_cbufs {
	// Recv side
	struct rping_rdma_info recv_buf;
	struct ibv_mr         *recv_mr;
	struct ibv_recv_wr    rq_wr;
	struct ibv_sge        recv_sgl;

	// Send side
	struct rping_rdma_info send_buf;
	struct ibv_mr         *send_mr;
	struct ibv_send_wr    sq_wr;
	struct ibv_sge        send_sgl;

	// RDMA side
	char                 *rdma_buf;
	struct ibv_mr        *rdma_mr;
	struct ibv_send_wr   rdma_sq_wr;
	struct ibv_sge       rdma_sgl;

	// Client start buffer
	char                 *start_buf;
	struct ibv_mr        *start_mr;
};

// Allocate rping_cbufs on C heap
static struct rping_cbufs *alloc_cbufs(void) {
	struct rping_cbufs *b = calloc(1, sizeof(struct rping_cbufs));
	return b;
}

static void free_cbufs(struct rping_cbufs *b) {
	if (b) free(b);
}

// Setup work requests (all pointers stay in C heap)
static void cbufs_setup_wr(struct rping_cbufs *b) {
	// Recv WR
	b->recv_sgl.addr = (uint64_t)(unsigned long)&b->recv_buf;
	b->recv_sgl.length = sizeof(struct rping_rdma_info);
	b->recv_sgl.lkey = b->recv_mr->lkey;
	b->rq_wr.sg_list = &b->recv_sgl;
	b->rq_wr.num_sge = 1;
	b->rq_wr.next = NULL;

	// Send WR
	b->send_sgl.addr = (uint64_t)(unsigned long)&b->send_buf;
	b->send_sgl.length = sizeof(struct rping_rdma_info);
	b->send_sgl.lkey = b->send_mr->lkey;
	b->sq_wr.opcode = IBV_WR_SEND;
	b->sq_wr.send_flags = IBV_SEND_SIGNALED;
	b->sq_wr.sg_list = &b->send_sgl;
	b->sq_wr.num_sge = 1;
	b->sq_wr.next = NULL;

	// RDMA WR
	b->rdma_sgl.addr = (uint64_t)(unsigned long)b->rdma_buf;
	b->rdma_sgl.lkey = b->rdma_mr->lkey;
	b->rdma_sq_wr.send_flags = IBV_SEND_SIGNALED;
	b->rdma_sq_wr.sg_list = &b->rdma_sgl;
	b->rdma_sq_wr.num_sge = 1;
	b->rdma_sq_wr.next = NULL;
}

// Register all memory regions
static int cbufs_register(struct rping_cbufs *b, struct ibv_pd *pd, int size, int is_server) {
	int rdma_access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;

	// Register recv buffer
	b->recv_mr = ibv_reg_mr(pd, &b->recv_buf, sizeof(struct rping_rdma_info), IBV_ACCESS_LOCAL_WRITE);
	if (!b->recv_mr) return errno;

	// Register send buffer
	b->send_mr = ibv_reg_mr(pd, &b->send_buf, sizeof(struct rping_rdma_info), 0);
	if (!b->send_mr) { int e = errno; ibv_dereg_mr(b->recv_mr); return e; }

	// Allocate and register rdma buffer
	b->rdma_buf = malloc(size);
	if (!b->rdma_buf) { ibv_dereg_mr(b->send_mr); ibv_dereg_mr(b->recv_mr); return ENOMEM; }
	b->rdma_mr = ibv_reg_mr(pd, b->rdma_buf, size, rdma_access);
	if (!b->rdma_mr) {
		int e = errno;
		free(b->rdma_buf); ibv_dereg_mr(b->send_mr); ibv_dereg_mr(b->recv_mr);
		return e;
	}

	// Client also needs start buffer
	if (!is_server) {
		b->start_buf = malloc(size);
		if (!b->start_buf) {
			ibv_dereg_mr(b->rdma_mr); free(b->rdma_buf);
			ibv_dereg_mr(b->send_mr); ibv_dereg_mr(b->recv_mr);
			return ENOMEM;
		}
		b->start_mr = ibv_reg_mr(pd, b->start_buf, size, rdma_access);
		if (!b->start_mr) {
			int e = errno;
			free(b->start_buf); ibv_dereg_mr(b->rdma_mr); free(b->rdma_buf);
			ibv_dereg_mr(b->send_mr); ibv_dereg_mr(b->recv_mr);
			return e;
		}
	}

	return 0;
}

// Deregister all memory regions
static void cbufs_deregister(struct rping_cbufs *b, int is_server) {
	if (b->recv_mr) { ibv_dereg_mr(b->recv_mr); b->recv_mr = NULL; }
	if (b->send_mr) { ibv_dereg_mr(b->send_mr); b->send_mr = NULL; }
	if (b->rdma_mr) { ibv_dereg_mr(b->rdma_mr); b->rdma_mr = NULL; }
	if (b->rdma_buf) { free(b->rdma_buf); b->rdma_buf = NULL; }
	if (!is_server) {
		if (b->start_mr) { ibv_dereg_mr(b->start_mr); b->start_mr = NULL; }
		if (b->start_buf) { free(b->start_buf); b->start_buf = NULL; }
	}
}

// Fill send_buf with RDMA info
static void cbufs_format_send(struct rping_cbufs *b, void *buf, struct ibv_mr *mr, int size) {
	b->send_buf.buf = htobe64((uint64_t)(unsigned long)buf);
	b->send_buf.rkey = htobe32(mr->rkey);
	b->send_buf.size = htobe32(size);
}

// Extract recv_buf info
static uint64_t cbufs_recv_get_buf(struct rping_cbufs *b) { return be64toh(b->recv_buf.buf); }
static uint32_t cbufs_recv_get_rkey(struct rping_cbufs *b) { return be32toh(b->recv_buf.rkey); }
static uint32_t cbufs_recv_get_size(struct rping_cbufs *b) { return be32toh(b->recv_buf.size); }

// Set RDMA read/write work request params
static void cbufs_set_rdma_read(struct rping_cbufs *b, uint32_t rkey, uint64_t remote_addr, uint32_t length) {
	b->rdma_sq_wr.opcode = IBV_WR_RDMA_READ;
	b->rdma_sq_wr.wr.rdma.rkey = rkey;
	b->rdma_sq_wr.wr.rdma.remote_addr = remote_addr;
	b->rdma_sgl.length = length;
}

static void cbufs_set_rdma_write(struct rping_cbufs *b, uint32_t rkey, uint64_t remote_addr, uint32_t length) {
	b->rdma_sq_wr.opcode = IBV_WR_RDMA_WRITE;
	b->rdma_sq_wr.wr.rdma.rkey = rkey;
	b->rdma_sq_wr.wr.rdma.remote_addr = remote_addr;
	b->rdma_sgl.length = length;
}

// Get rdma_buf strlen for write
static uint32_t cbufs_rdma_strlen(struct rping_cbufs *b) {
	return (uint32_t)(strlen(b->rdma_buf) + 1);
}

// Post recv
static int cbufs_post_recv(struct rping_cbufs *b, struct ibv_qp *qp) {
	struct ibv_recv_wr *bad_wr;
	return ibv_post_recv(qp, &b->rq_wr, &bad_wr);
}

// Post send
static int cbufs_post_send(struct rping_cbufs *b, struct ibv_qp *qp) {
	struct ibv_send_wr *bad_wr;
	return ibv_post_send(qp, &b->sq_wr, &bad_wr);
}

// Post rdma WR
static int cbufs_post_rdma(struct rping_cbufs *b, struct ibv_qp *qp) {
	struct ibv_send_wr *bad_wr;
	return ibv_post_send(qp, &b->rdma_sq_wr, &bad_wr);
}

// --- Helper functions ---

static int get_channel_fd(struct rdma_event_channel *ch) { return ch->fd; }
static struct ibv_context *get_verbs(struct rdma_cm_id *id) { return id->verbs; }
static uint8_t get_port_num(struct rdma_cm_id *id) { return id->port_num; }
static struct ibv_qp *get_cm_id_qp(struct rdma_cm_id *id) { return id->qp; }
static struct rdma_cm_id *get_event_id(struct rdma_cm_event *event) { return event->id; }
static int get_event_event(struct rdma_cm_event *event) { return event->event; }
static int get_event_status(struct rdma_cm_event *event) { return event->status; }
static const char *event_str_wrapper(int event) { return rdma_event_str(event); }
static uint32_t get_qp_num(struct ibv_qp *qp) { return qp->qp_num; }

static void set_sockaddr_port(struct sockaddr *addr, uint16_t port) {
	if (addr->sa_family == AF_INET)
		((struct sockaddr_in *)addr)->sin_port = port;
	else
		((struct sockaddr_in6 *)addr)->sin6_port = port;
}

static int resolve_addr(char *dst, struct sockaddr *addr) {
	struct addrinfo *res;
	int ret = getaddrinfo(dst, NULL, NULL, &res);
	if (ret) return ret;
	if (res->ai_family == PF_INET)
		memcpy(addr, res->ai_addr, sizeof(struct sockaddr_in));
	else if (res->ai_family == PF_INET6)
		memcpy(addr, res->ai_addr, sizeof(struct sockaddr_in6));
	else
		ret = -1;
	freeaddrinfo(res);
	return ret;
}

static void init_conn_param(struct rdma_conn_param *param, int qp_num, int self_create_qp) {
	memset(param, 0, sizeof(*param));
	param->responder_resources = 1;
	param->initiator_depth = 1;
	param->retry_count = 7;
	param->rnr_retry_count = 7;
	if (self_create_qp)
		param->qp_num = qp_num;
}

static int self_modify_qp(struct ibv_qp *qp, struct rdma_cm_id *id) {
	struct ibv_qp_attr qp_attr;
	int qp_attr_mask, ret;
	qp_attr.qp_state = IBV_QPS_INIT;
	ret = rdma_init_qp_attr(id, &qp_attr, &qp_attr_mask);
	if (ret) return ret;
	ret = ibv_modify_qp(qp, &qp_attr, qp_attr_mask);
	if (ret) return ret;
	qp_attr.qp_state = IBV_QPS_RTR;
	ret = rdma_init_qp_attr(id, &qp_attr, &qp_attr_mask);
	if (ret) return ret;
	ret = ibv_modify_qp(qp, &qp_attr, qp_attr_mask);
	if (ret) return ret;
	qp_attr.qp_state = IBV_QPS_RTS;
	ret = rdma_init_qp_attr(id, &qp_attr, &qp_attr_mask);
	if (ret) return ret;
	return ibv_modify_qp(qp, &qp_attr, qp_attr_mask);
}

static int create_event_channel_wrapper(struct rdma_event_channel **ch) {
	*ch = rdma_create_event_channel();
	if (!*ch) return errno;
	return 0;
}

static int alloc_pd_wrapper(struct ibv_context *ctx, struct ibv_pd **pd) {
	*pd = ibv_alloc_pd(ctx);
	if (!*pd) return errno;
	return 0;
}

static int create_comp_channel_wrapper(struct ibv_context *ctx, struct ibv_comp_channel **ch) {
	*ch = ibv_create_comp_channel(ctx);
	if (!*ch) return errno;
	return 0;
}

static int create_cq_wrapper(struct ibv_context *ctx, int cqe, struct ibv_comp_channel *ch,
                             struct ibv_cq **cq) {
	*cq = ibv_create_cq(ctx, cqe, NULL, ch, 0);
	if (!*cq) return errno;
	return 0;
}

static int create_qp_wrapper(struct ibv_pd *pd, struct ibv_qp_init_attr *attr, struct ibv_qp **qp) {
	*qp = ibv_create_qp(pd, attr);
	if (!*qp) return errno;
	return 0;
}

static void getnameinfo_wrapper(struct sockaddr *addr, socklen_t addrlen,
                                char *host, socklen_t hostlen) {
	getnameinfo(addr, addrlen, host, hostlen, NULL, 0, NI_NUMERICHOST);
}

static uint16_t go_htobe16(uint16_t v) { return htobe16(v); }
static uint16_t go_be16toh(uint16_t v) { return be16toh(v); }

static int poll_wrapper(struct pollfd *fds, nfds_t nfds, int timeout) {
	int ret;
	do { ret = poll(fds, nfds, timeout); } while (ret == -1 && errno == EINTR);
	if (ret == -1) return -errno;
	return ret;
}

static int get_cm_event_wrapper(struct rdma_event_channel *channel, struct rdma_cm_event **event) {
	int ret = rdma_get_cm_event(channel, event);
	if (ret) return -errno;
	return 0;
}

static int get_cq_event_wrapper(struct ibv_comp_channel *channel, struct ibv_cq **cq, void **ctx) {
	int ret = ibv_get_cq_event(channel, cq, ctx);
	if (ret) return -errno;
	return 0;
}

// Validate recv_buf size
static int cbufs_recv_size_ok(struct rping_cbufs *b, uint32_t byte_len) {
	return byte_len == sizeof(struct rping_rdma_info);
}
*/
import "C"
import (
	"flag"
	"fmt"
	"net"
	"os"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

// --- Constants ---
const (
	rpingBufSize    = 64 * 1024
	rpingSQDepth    = 16
	rpingMinBufSize = 16
	rpingMsgFmt     = "rdma-ping-%d: "
	defaultPort     = 7174
)

// --- TestState ---
type TestState int

const (
	StateIdle             TestState = iota + 1
	StateConnectRequest
	StateAddrResolved
	StateRouteResolved
	StateConnected
	StateRDMAReadAdv
	StateRDMAReadComplete
	StateRDMAWriteAdv
	StateRDMAWriteComplete
	StateDisconnected
	StateError
)

func (s TestState) String() string {
	names := map[TestState]string{
		StateIdle: "IDLE", StateConnectRequest: "CONNECT_REQUEST",
		StateAddrResolved: "ADDR_RESOLVED", StateRouteResolved: "ROUTE_RESOLVED",
		StateConnected: "CONNECTED", StateRDMAReadAdv: "RDMA_READ_ADV",
		StateRDMAReadComplete: "RDMA_READ_COMPLETE", StateRDMAWriteAdv: "RDMA_WRITE_ADV",
		StateRDMAWriteComplete: "RDMA_WRITE_COMPLETE", StateDisconnected: "DISCONNECTED",
		StateError: "ERROR",
	}
	if n, ok := names[s]; ok {
		return n
	}
	return fmt.Sprintf("UNKNOWN(%d)", s)
}

// --- RPingCB ---
type RPingCB struct {
	mu    sync.Mutex
	cond  *sync.Cond
	state TestState

	server bool

	// RDMA resources
	pd      *C.struct_ibv_pd
	channel *C.struct_ibv_comp_channel
	cq      *C.struct_ibv_cq
	qp      *C.struct_ibv_qp

	// C-heap allocated buffers and work requests
	cbufs *C.struct_rping_cbufs

	// Remote RDMA info
	remoteRkey uint32
	remoteAddr uint64
	remoteLen  uint32

	// CM
	cmChannel *C.struct_rdma_event_channel
	cmID      *C.struct_rdma_cm_id
	childCMID *C.struct_rdma_cm_id

	// Address
	sin     C.struct_sockaddr_storage
	ssource C.struct_sockaddr_storage
	port    uint16

	// Options
	verbose      int
	selfCreateQP bool
	count        int
	size         int
	validate     bool

	// Event fd
	eventfd C.int

	// Goroutine done channels
	cqDone chan struct{}
	cmDone chan struct{}
}

func newRPingCB() *RPingCB {
	cb := &RPingCB{
		state:  StateIdle,
		size:   64,
		port:   uint16(C.go_htobe16(C.ushort(defaultPort))),
		cqDone: make(chan struct{}),
		cmDone: make(chan struct{}),
	}
	cb.sin.ss_family = C.PF_INET
	cb.cond = sync.NewCond(&cb.mu)
	return cb
}

func (cb *RPingCB) setState(s TestState) {
	cb.mu.Lock()
	cb.state = s
	cb.cond.Broadcast()
	cb.mu.Unlock()
}

func (cb *RPingCB) getState() TestState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

func (cb *RPingCB) waitState(targets ...TestState) TestState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	for {
		for _, t := range targets {
			if cb.state == t {
				return cb.state
			}
		}
		cb.cond.Wait()
	}
}

// --- Byte order helpers ---
func htobe16(v uint16) uint16 { return uint16(C.go_htobe16(C.ushort(v))) }
func be16toh(v uint16) uint16 { return uint16(C.go_be16toh(C.ushort(v))) }

// --- Address resolution ---
func resolveAddr(dst string) (C.struct_sockaddr_storage, error) {
	cDst := C.CString(dst)
	defer C.free(unsafe.Pointer(cDst))
	var addr C.struct_sockaddr_storage
	ret := C.resolve_addr(cDst, (*C.struct_sockaddr)(unsafe.Pointer(&addr)))
	if ret != 0 {
		return addr, fmt.Errorf("resolve_addr failed for %s", dst)
	}
	return addr, nil
}

// --- CM event handler ---
func (cb *RPingCB) cmaEventHandler(cmaID *C.struct_rdma_cm_id, event *C.struct_rdma_cm_event) C.int {
	eventType := C.get_event_event(event)

	switch eventType {
	case C.RDMA_CM_EVENT_ADDR_RESOLVED:
		cb.setState(StateAddrResolved)
		ret := C.rdma_resolve_route(cmaID, 2000)
		if ret != 0 {
			cb.setState(StateError)
		}

	case C.RDMA_CM_EVENT_ROUTE_RESOLVED:
		cb.setState(StateRouteResolved)

	case C.RDMA_CM_EVENT_CONNECT_REQUEST:
		cb.mu.Lock()
		cb.childCMID = cmaID
		cb.state = StateConnectRequest
		cb.cond.Broadcast()
		cb.mu.Unlock()

	case C.RDMA_CM_EVENT_CONNECT_RESPONSE:
		cb.setState(StateConnected)

	case C.RDMA_CM_EVENT_ESTABLISHED:
		if !cb.server {
			cb.setState(StateConnected)
		} else {
			cb.mu.Lock()
			cb.cond.Broadcast()
			cb.mu.Unlock()
		}

	case C.RDMA_CM_EVENT_ADDR_ERROR, C.RDMA_CM_EVENT_ROUTE_ERROR,
		C.RDMA_CM_EVENT_CONNECT_ERROR, C.RDMA_CM_EVENT_UNREACHABLE,
		C.RDMA_CM_EVENT_REJECTED:
		fmt.Fprintf(os.Stderr, "cma event %s, error %d\n",
			C.GoString(C.event_str_wrapper(eventType)), C.get_event_status(event))
		cb.mu.Lock()
		cb.cond.Broadcast()
		cb.mu.Unlock()
		return -1

	case C.RDMA_CM_EVENT_DISCONNECTED:
		role := "client"
		if cb.server {
			role = "server"
		}
		fmt.Fprintf(os.Stderr, "%s DISCONNECT EVENT...\n", role)
		cb.setState(StateDisconnected)

	case C.RDMA_CM_EVENT_DEVICE_REMOVAL:
		fmt.Fprintf(os.Stderr, "cma detected device removal!!!!\n")
		cb.setState(StateError)
		return -1

	default:
		fmt.Fprintf(os.Stderr, "unhandled event: %s, ignoring\n", C.GoString(C.event_str_wrapper(eventType)))
	}

	return 0
}

// --- CQ event handler ---
func (cb *RPingCB) cqEventHandler() C.int {
	var wc C.struct_ibv_wc
	var flushed C.int = 0

	for {
		ret := C.ibv_poll_cq(cb.cq, 1, &wc)
		if ret == 0 {
			break
		}
		if ret < 0 {
			fmt.Fprintf(os.Stderr, "poll error %d\n", ret)
			cb.setState(StateError)
			return C.int(ret)
		}

		if wc.status != 0 {
			if wc.status == C.IBV_WC_WR_FLUSH_ERR {
				flushed = 1
				continue
			}
			fmt.Fprintf(os.Stderr, "cq completion failed status %d\n", wc.status)
			cb.setState(StateError)
			return -1
		}

		switch wc.opcode {
		case C.IBV_WC_SEND:
			// nothing

		case C.IBV_WC_RDMA_WRITE:
			cb.setState(StateRDMAWriteComplete)

		case C.IBV_WC_RDMA_READ:
			cb.setState(StateRDMAReadComplete)

		case C.IBV_WC_RECV:
			if C.cbufs_recv_size_ok(cb.cbufs, C.uint32_t(wc.byte_len)) == 0 {
				fmt.Fprintf(os.Stderr, "Received bogus data, size %d\n", wc.byte_len)
				cb.setState(StateError)
				return -1
			}

			if cb.server {
				cb.remoteRkey = uint32(C.cbufs_recv_get_rkey(cb.cbufs))
				cb.remoteAddr = uint64(C.cbufs_recv_get_buf(cb.cbufs))
				cb.remoteLen = uint32(C.cbufs_recv_get_size(cb.cbufs))

				state := cb.getState()
				if state <= StateConnected || state == StateRDMAWriteComplete {
					cb.setState(StateRDMAReadAdv)
				} else {
					cb.setState(StateRDMAWriteAdv)
				}
			} else {
				state := cb.getState()
				if state == StateRDMAReadAdv {
					cb.setState(StateRDMAWriteAdv)
				} else {
					cb.setState(StateRDMAWriteComplete)
				}
			}

			ret := C.cbufs_post_recv(cb.cbufs, cb.qp)
			if ret != 0 {
				fmt.Fprintf(os.Stderr, "post recv error: %d\n", ret)
				cb.setState(StateError)
				return -1
			}

		default:
			fmt.Fprintf(os.Stderr, "unknown completion opcode %d\n", wc.opcode)
			cb.setState(StateError)
			return -1
		}
	}

	return flushed
}

// --- QP setup ---
func (cb *RPingCB) setupQP(cmID *C.struct_rdma_cm_id) error {
	verbs := C.get_verbs(cmID)

	var pd *C.struct_ibv_pd
	if ret := C.alloc_pd_wrapper(verbs, &pd); ret != 0 {
		return fmt.Errorf("ibv_alloc_pd failed: %d", ret)
	}
	cb.pd = pd

	var ch *C.struct_ibv_comp_channel
	if ret := C.create_comp_channel_wrapper(verbs, &ch); ret != 0 {
		C.ibv_dealloc_pd(cb.pd)
		return fmt.Errorf("ibv_create_comp_channel failed: %d", ret)
	}
	cb.channel = ch

	var cq *C.struct_ibv_cq
	if ret := C.create_cq_wrapper(verbs, rpingSQDepth*2, cb.channel, &cq); ret != 0 {
		C.ibv_destroy_comp_channel(cb.channel)
		C.ibv_dealloc_pd(cb.pd)
		return fmt.Errorf("ibv_create_cq failed: %d", ret)
	}
	cb.cq = cq

	if ret := C.ibv_req_notify_cq(cb.cq, 0); ret != 0 {
		C.ibv_destroy_cq(cb.cq)
		C.ibv_destroy_comp_channel(cb.channel)
		C.ibv_dealloc_pd(cb.pd)
		return fmt.Errorf("ibv_req_notify_cq failed")
	}

	if err := cb.createQP(cmID); err != nil {
		C.ibv_destroy_cq(cb.cq)
		C.ibv_destroy_comp_channel(cb.channel)
		C.ibv_dealloc_pd(cb.pd)
		return err
	}

	return nil
}

func (cb *RPingCB) createQP(cmID *C.struct_rdma_cm_id) error {
	var initAttr C.struct_ibv_qp_init_attr
	C.memset(unsafe.Pointer(&initAttr), 0, C.sizeof_struct_ibv_qp_init_attr)
	initAttr.cap.max_send_wr = rpingSQDepth
	initAttr.cap.max_recv_wr = 2
	initAttr.cap.max_recv_sge = 1
	initAttr.cap.max_send_sge = 1
	initAttr.qp_type = C.IBV_QPT_RC
	initAttr.send_cq = cb.cq
	initAttr.recv_cq = cb.cq

	var id *C.struct_rdma_cm_id
	if cb.server {
		id = cb.childCMID
	} else {
		id = cb.cmID
	}

	if cb.selfCreateQP {
		var qp *C.struct_ibv_qp
		if ret := C.create_qp_wrapper(cb.pd, &initAttr, &qp); ret != 0 {
			return fmt.Errorf("ibv_create_qp failed: %d", ret)
		}
		cb.qp = qp

		var attr C.struct_ibv_qp_attr
		C.memset(unsafe.Pointer(&attr), 0, C.sizeof_struct_ibv_qp_attr)
		attr.qp_state = C.IBV_QPS_INIT
		attr.pkey_index = 0
		attr.port_num = C.get_port_num(id)
		attr.qp_access_flags = 0

		ret := C.ibv_modify_qp(cb.qp, &attr,
			C.IBV_QP_STATE|C.IBV_QP_PKEY_INDEX|C.IBV_QP_PORT|C.IBV_QP_ACCESS_FLAGS)
		if ret != 0 {
			C.ibv_destroy_qp(cb.qp)
			return fmt.Errorf("ibv_modify_qp to INIT failed")
		}
		return nil
	}

	ret := C.rdma_create_qp(id, cb.pd, &initAttr)
	if ret != 0 {
		return fmt.Errorf("rdma_create_qp failed")
	}
	cb.qp = C.get_cm_id_qp(id)
	return nil
}

func (cb *RPingCB) freeQP() {
	if cb.qp != nil {
		C.ibv_destroy_qp(cb.qp)
		cb.qp = nil
	}
	if cb.cq != nil {
		C.ibv_destroy_cq(cb.cq)
		cb.cq = nil
	}
	if cb.channel != nil {
		C.ibv_destroy_comp_channel(cb.channel)
		cb.channel = nil
	}
	if cb.pd != nil {
		C.ibv_dealloc_pd(cb.pd)
		cb.pd = nil
	}
}

// --- Buffer setup (all on C heap) ---
func (cb *RPingCB) setupBuffers() error {
	cb.cbufs = C.alloc_cbufs()
	if cb.cbufs == nil {
		return fmt.Errorf("alloc_cbufs failed")
	}

	isServer := C.int(0)
	if cb.server {
		isServer = 1
	}

	ret := C.cbufs_register(cb.cbufs, cb.pd, C.int(cb.size), isServer)
	if ret != 0 {
		C.free_cbufs(cb.cbufs)
		cb.cbufs = nil
		return fmt.Errorf("cbufs_register failed: %d", ret)
	}

	C.cbufs_setup_wr(cb.cbufs)
	return nil
}

func (cb *RPingCB) freeBuffers() {
	if cb.cbufs != nil {
		isServer := C.int(0)
		if cb.server {
			isServer = 1
		}
		C.cbufs_deregister(cb.cbufs, isServer)
		C.free_cbufs(cb.cbufs)
		cb.cbufs = nil
	}
}

// --- Send/Recv helpers ---
func (cb *RPingCB) postRecv() error {
	ret := C.cbufs_post_recv(cb.cbufs, cb.qp)
	if ret != 0 {
		return fmt.Errorf("ibv_post_recv failed: %d", ret)
	}
	return nil
}

func (cb *RPingCB) postSend() error {
	ret := C.cbufs_post_send(cb.cbufs, cb.qp)
	if ret != 0 {
		return fmt.Errorf("ibv_post_send failed: %d", ret)
	}
	return nil
}

func (cb *RPingCB) postRdmaWR() error {
	ret := C.cbufs_post_rdma(cb.cbufs, cb.qp)
	if ret != 0 {
		return fmt.Errorf("ibv_post_send (rdma) failed: %d", ret)
	}
	return nil
}

// --- Connection management ---
func (cb *RPingCB) accept() error {
	if cb.selfCreateQP {
		ret := C.self_modify_qp(cb.qp, cb.childCMID)
		if ret != 0 {
			return fmt.Errorf("self_modify_qp failed")
		}
		var connParam C.struct_rdma_conn_param
		C.init_conn_param(&connParam, C.int(C.get_qp_num(cb.qp)), 1)
		ret = C.rdma_accept(cb.childCMID, &connParam)
		if ret != 0 {
			return fmt.Errorf("rdma_accept failed")
		}
	} else {
		ret := C.rdma_accept(cb.childCMID, nil)
		if ret != 0 {
			return fmt.Errorf("rdma_accept failed")
		}
	}

	s := cb.waitState(StateConnected, StateError)
	if s != StateConnected {
		return fmt.Errorf("wait for CONNECTED state %s", s)
	}
	return nil
}

func (cb *RPingCB) disconnect(id *C.struct_rdma_cm_id) error {
	if cb.selfCreateQP {
		var qpAttr C.struct_ibv_qp_attr
		C.memset(unsafe.Pointer(&qpAttr), 0, C.sizeof_struct_ibv_qp_attr)
		qpAttr.qp_state = C.IBV_QPS_ERR
		C.ibv_modify_qp(cb.qp, &qpAttr, C.IBV_QP_STATE)
	}
	ret := C.rdma_disconnect(id)
	if ret != 0 {
		return fmt.Errorf("rdma_disconnect failed")
	}
	return nil
}

func (cb *RPingCB) connectClient() error {
	var connParam C.struct_rdma_conn_param
	qpNum := C.int(0)
	if cb.selfCreateQP && cb.qp != nil {
		qpNum = C.int(C.get_qp_num(cb.qp))
	}
	selfQP := C.int(0)
	if cb.selfCreateQP {
		selfQP = 1
	}
	C.init_conn_param(&connParam, qpNum, selfQP)

	ret := C.rdma_connect(cb.cmID, &connParam)
	if ret != 0 {
		return fmt.Errorf("rdma_connect failed")
	}

	s := cb.waitState(StateConnected, StateError)
	if s != StateConnected {
		return fmt.Errorf("wait for CONNECTED state %s", s)
	}

	if cb.selfCreateQP {
		ret = C.self_modify_qp(cb.qp, cb.cmID)
		if ret != 0 {
			return fmt.Errorf("self_modify_qp failed")
		}
		ret = C.rdma_establish(cb.cmID)
		if ret != 0 {
			return fmt.Errorf("rdma_establish failed")
		}
	}

	return nil
}

func (cb *RPingCB) bindClient() error {
	C.set_sockaddr_port((*C.struct_sockaddr)(unsafe.Pointer(&cb.sin)), C.ushort(cb.port))

	var ret C.int
	if cb.ssource.ss_family != 0 {
		ret = C.rdma_resolve_addr(cb.cmID,
			(*C.struct_sockaddr)(unsafe.Pointer(&cb.ssource)),
			(*C.struct_sockaddr)(unsafe.Pointer(&cb.sin)), 2000)
	} else {
		ret = C.rdma_resolve_addr(cb.cmID, nil,
			(*C.struct_sockaddr)(unsafe.Pointer(&cb.sin)), 2000)
	}
	if ret != 0 {
		return fmt.Errorf("rdma_resolve_addr failed")
	}

	s := cb.waitState(StateRouteResolved, StateError)
	if s != StateRouteResolved {
		return fmt.Errorf("waiting for addr/route resolution state %s", s)
	}
	return nil
}

func (cb *RPingCB) bindServer() error {
	C.set_sockaddr_port((*C.struct_sockaddr)(unsafe.Pointer(&cb.sin)), C.ushort(cb.port))

	ret := C.rdma_bind_addr(cb.cmID, (*C.struct_sockaddr)(unsafe.Pointer(&cb.sin)))
	if ret != 0 {
		return fmt.Errorf("rdma_bind_addr failed")
	}

	ret = C.rdma_listen(cb.cmID, 3)
	if ret != 0 {
		return fmt.Errorf("rdma_listen failed")
	}
	return nil
}

func (cb *RPingCB) getAddrString() string {
	var addrStr [C.INET6_ADDRSTRLEN]byte
	C.getnameinfo_wrapper(
		(*C.struct_sockaddr)(unsafe.Pointer(&cb.sin)),
		C.socklen_t(C.sizeof_struct_sockaddr_storage),
		(*C.char)(unsafe.Pointer(&addrStr[0])),
		C.INET6_ADDRSTRLEN)
	return C.GoString((*C.char)(unsafe.Pointer(&addrStr[0])))
}

// --- CM thread ---
func cmThread(cb *RPingCB, stopCh chan struct{}) {
	defer close(cb.cmDone)

	var pfds [2]C.struct_pollfd
	pfds[0].fd = cb.eventfd
	pfds[0].events = C.POLLIN
	pfds[1].fd = C.get_channel_fd(cb.cmChannel)
	pfds[1].events = C.POLLIN

	for {
		ret := C.poll_wrapper(&pfds[0], 2, -1)
		if ret < 0 {
			fmt.Fprintf(os.Stderr, "poll failed: %v\n", syscall.Errno(-ret))
			return
		}
		if ret == 0 {
			continue
		}

		if pfds[0].revents&C.POLLIN != 0 {
			return
		}

		if pfds[1].revents&C.POLLIN != 0 {
			var event *C.struct_rdma_cm_event
			ret := C.get_cm_event_wrapper(cb.cmChannel, &event)
			if ret != 0 {
				fmt.Fprintf(os.Stderr, "rdma_get_cm_event: %v\n", syscall.Errno(-ret))
				return
			}
			cmaID := C.get_event_id(event)
			handlerRet := cb.cmaEventHandler(cmaID, event)
			C.rdma_ack_cm_event(event)
			if handlerRet != 0 {
				return
			}
		}

		select {
		case <-stopCh:
			return
		default:
		}
	}
}

// --- CQ thread ---
func cqThread(cb *RPingCB, stopCh chan struct{}) {
	defer close(cb.cqDone)

	for {
		select {
		case <-stopCh:
			return
		default:
		}

		var evCQ *C.struct_ibv_cq
		var evCtx unsafe.Pointer

		ret := C.get_cq_event_wrapper(cb.channel, &evCQ, &evCtx)
		if ret != 0 {
			fmt.Fprintf(os.Stderr, "Failed to get cq event!\n")
			return
		}
		if evCQ != cb.cq {
			fmt.Fprintf(os.Stderr, "Unknown CQ!\n")
			return
		}

		if ret = C.ibv_req_notify_cq(cb.cq, 0); ret != 0 {
			fmt.Fprintf(os.Stderr, "Failed to set notify!\n")
			return
		}

		handlerRet := cb.cqEventHandler()
		C.ibv_ack_cq_events(cb.cq, 1)
		if handlerRet != 0 {
			fmt.Fprintf(os.Stderr, "CQ event handler returned %d\n", handlerRet)
			return
		}
	}
}

// rdmaBufPtr returns a Go slice backed by the C-heap rdma_buf
func (cb *RPingCB) rdmaBufSlice() []byte {
	return C.GoBytes(unsafe.Pointer(cb.cbufs.rdma_buf), C.int(cb.size))
}

// startBufPtr returns a Go slice backed by the C-heap start_buf
func (cb *RPingCB) startBufSlice() []byte {
	return C.GoBytes(unsafe.Pointer(cb.cbufs.start_buf), C.int(cb.size))
}

// copyToRdmaBuf copies data into the C-heap rdma_buf
func (cb *RPingCB) copyToRdmaBuf(src []byte) {
	dst := cb.rdmaBufSlice()
	copy(dst, src)
}

// copyToStartBuf copies data into the C-heap start_buf
func (cb *RPingCB) copyToStartBuf(src []byte) {
	dst := cb.startBufSlice()
	copy(dst, src)
}

// --- Server ping/pong loop ---
func testServer(cb *RPingCB) int {
	for {
		s := cb.waitState(StateRDMAReadAdv, StateDisconnected, StateError)
		if s != StateRDMAReadAdv {
			if s == StateDisconnected {
				return 0
			}
			fmt.Fprintf(os.Stderr, "wait for RDMA_READ_ADV state %s\n", s)
			return -1
		}

		// Issue RDMA Read
		C.cbufs_set_rdma_read(cb.cbufs,
			C.uint32_t(cb.remoteRkey), C.uint64_t(cb.remoteAddr), C.uint32_t(cb.remoteLen))
		if err := cb.postRdmaWR(); err != nil {
			fmt.Fprintf(os.Stderr, "post rdma read error: %v\n", err)
			break
		}

		// Wait for read completion
		s = cb.waitState(StateRDMAReadComplete, StateDisconnected, StateError)
		if s != StateRDMAReadComplete {
			fmt.Fprintf(os.Stderr, "wait for RDMA_READ_COMPLETE state %s\n", s)
			return -1
		}

		// Display data
		if cb.verbose > 0 {
			rdmaStr := C.GoStringN(cb.cbufs.rdma_buf, C.int(cb.size))
			fmt.Printf("server ping data: %s\n", rdmaStr)
		}

		// Tell client to continue
		if err := cb.postSend(); err != nil {
			fmt.Fprintf(os.Stderr, "post send error: %v\n", err)
			break
		}

		// Wait for client's RDMA STAG/TO/Len
		s = cb.waitState(StateRDMAWriteAdv, StateDisconnected, StateError)
		if s != StateRDMAWriteAdv {
			fmt.Fprintf(os.Stderr, "wait for RDMA_WRITE_ADV state %s\n", s)
			return -1
		}

		// RDMA Write echo data
		writeLen := uint32(C.cbufs_rdma_strlen(cb.cbufs))
		C.cbufs_set_rdma_write(cb.cbufs,
			C.uint32_t(cb.remoteRkey), C.uint64_t(cb.remoteAddr), C.uint32_t(writeLen))
		if err := cb.postRdmaWR(); err != nil {
			fmt.Fprintf(os.Stderr, "post rdma write error: %v\n", err)
			break
		}

		// Wait for write completion
		s = cb.waitState(StateRDMAWriteComplete, StateDisconnected, StateError)
		if s != StateRDMAWriteComplete {
			fmt.Fprintf(os.Stderr, "wait for RDMA_WRITE_COMPLETE state %s\n", s)
			return -1
		}

		// Tell client to begin again
		if err := cb.postSend(); err != nil {
			fmt.Fprintf(os.Stderr, "post send error: %v\n", err)
			break
		}
	}

	if cb.getState() == StateDisconnected {
		return 0
	}
	return -1
}

// --- Client ping/pong loop ---
func testClient(cb *RPingCB) int {
	var rttMin, rttMax, rttSum float64
	packetsSent := 0
	packetsReceived := 0
	startChar := byte(65)

	totalStart := time.Now()

	for ping := 0; cb.count == 0 || ping < cb.count; ping++ {
		cb.setState(StateRDMAReadAdv)
		packetsSent++

		// Fill start_buf with ascii text
		cc := fmt.Sprintf(rpingMsgFmt, ping)
		startSlice := cb.startBufSlice()
		copy(startSlice, []byte(cc))
		c := startChar
		for i := len(cc); i < cb.size; i++ {
			startSlice[i] = c
			c++
			if c > 122 {
				c = 65
			}
		}
		startChar++
		if startChar > 122 {
			startChar = 65
		}
		startSlice[cb.size-1] = 0

		pingStart := time.Now()

		// Send source rkey/addr/len to server
		C.cbufs_format_send(cb.cbufs, unsafe.Pointer(cb.cbufs.start_buf), cb.cbufs.start_mr, C.int(cb.size))
		if err := cb.postSend(); err != nil {
			fmt.Fprintf(os.Stderr, "post send error: %v\n", err)
			break
		}

		// Wait for server to ACK
		s := cb.waitState(StateRDMAWriteAdv, StateDisconnected, StateError)
		if s != StateRDMAWriteAdv {
			fmt.Fprintf(os.Stderr, "wait for RDMA_WRITE_ADV state %s\n", s)
			return -1
		}

		// Send sink rkey/addr/len to server
		C.cbufs_format_send(cb.cbufs, unsafe.Pointer(cb.cbufs.rdma_buf), cb.cbufs.rdma_mr, C.int(cb.size))
		if err := cb.postSend(); err != nil {
			fmt.Fprintf(os.Stderr, "post send error: %v\n", err)
			break
		}

		// Wait for RDMA Write complete
		s = cb.waitState(StateRDMAWriteComplete, StateDisconnected, StateError)
		if s != StateRDMAWriteComplete {
			fmt.Fprintf(os.Stderr, "wait for RDMA_WRITE_COMPLETE state %s\n", s)
			return -1
		}

		// Validate data
		if cb.validate {
			startSlice = cb.startBufSlice()
			rdmaSlice := cb.rdmaBufSlice()
			for i := 0; i < cb.size; i++ {
				if startSlice[i] != rdmaSlice[i] {
					fmt.Fprintf(os.Stderr, "data mismatch at offset %d!\n", i)
					return -1
				}
			}
		}

		if cb.verbose > 0 {
			rdmaStr := C.GoStringN(cb.cbufs.rdma_buf, C.int(cb.size))
			fmt.Printf("ping data: %s\n", rdmaStr)
		}

		pingEnd := time.Now()
		elapsedMs := float64(pingEnd.Sub(pingStart)) / float64(time.Millisecond)

		addrStr := cb.getAddrString()
		fmt.Printf("rdma-ping-%d: %s request took %.3f ms\n", ping, addrStr, elapsedMs)

		packetsReceived++
		if packetsReceived == 1 {
			rttMin = elapsedMs
			rttMax = elapsedMs
		} else {
			if elapsedMs < rttMin {
				rttMin = elapsedMs
			}
			if elapsedMs > rttMax {
				rttMax = elapsedMs
			}
		}
		rttSum += elapsedMs
	}

	totalEnd := time.Now()
	totalTimeMs := float64(totalEnd.Sub(totalStart)) / float64(time.Millisecond)

	fmt.Printf("\n--- rping statistics ---\n")
	packetLoss := 0
	if packetsSent > 0 {
		packetLoss = (packetsSent - packetsReceived) * 100 / packetsSent
	}
	fmt.Printf("%d packets transmitted, %d received, %d%% packet loss, time %.0f ms\n",
		packetsSent, packetsReceived, packetLoss, totalTimeMs)
	if packetsReceived > 0 {
		fmt.Printf("rtt min/avg/max = %.3f/%.3f/%.3f ms\n",
			rttMin, rttSum/float64(packetsReceived), rttMax)
	}

	if cb.getState() == StateDisconnected {
		return 0
	}
	return 0
}

// --- Run client ---
func runClient(cb *RPingCB) int {
	if err := cb.bindClient(); err != nil {
		fmt.Fprintf(os.Stderr, "bind_client failed: %v\n", err)
		return -1
	}

	if err := cb.setupQP(cb.cmID); err != nil {
		fmt.Fprintf(os.Stderr, "setup_qp failed: %v\n", err)
		return -1
	}

	if err := cb.setupBuffers(); err != nil {
		fmt.Fprintf(os.Stderr, "rping_setup_buffers failed: %v\n", err)
		cb.freeQP()
		return -1
	}

	if err := cb.postRecv(); err != nil {
		fmt.Fprintf(os.Stderr, "ibv_post_recv failed: %v\n", err)
		cb.freeBuffers()
		cb.freeQP()
		return -1
	}

	cqStopCh := make(chan struct{})
	cb.cqDone = make(chan struct{})
	go cqThread(cb, cqStopCh)

	if err := cb.connectClient(); err != nil {
		fmt.Fprintf(os.Stderr, "connect error: %v\n", err)
		close(cqStopCh)
		<-cb.cqDone
		cb.freeBuffers()
		cb.freeQP()
		return -1
	}

	ret := testClient(cb)

	cb.disconnect(cb.cmID)
	close(cqStopCh)
	<-cb.cqDone
	cb.freeBuffers()
	cb.freeQP()
	return ret
}

// --- Run server (single connection) ---
func runServer(cb *RPingCB) int {
	if err := cb.bindServer(); err != nil {
		fmt.Fprintf(os.Stderr, "bind_server failed: %v\n", err)
		return -1
	}

	s := cb.waitState(StateConnectRequest, StateError)
	if s != StateConnectRequest {
		fmt.Fprintf(os.Stderr, "wait for CONNECT_REQUEST state %s\n", s)
		return -1
	}

	if err := cb.setupQP(cb.childCMID); err != nil {
		fmt.Fprintf(os.Stderr, "setup_qp failed: %v\n", err)
		return -1
	}

	if err := cb.setupBuffers(); err != nil {
		fmt.Fprintf(os.Stderr, "rping_setup_buffers failed: %v\n", err)
		cb.freeQP()
		return -1
	}

	if err := cb.postRecv(); err != nil {
		fmt.Fprintf(os.Stderr, "ibv_post_recv failed: %v\n", err)
		cb.freeBuffers()
		cb.freeQP()
		return -1
	}

	cqStopCh := make(chan struct{})
	cb.cqDone = make(chan struct{})
	go cqThread(cb, cqStopCh)

	if err := cb.accept(); err != nil {
		fmt.Fprintf(os.Stderr, "connect error: %v\n", err)
		close(cqStopCh)
		<-cb.cqDone
		cb.freeBuffers()
		cb.freeQP()
		return -1
	}

	ret := testServer(cb)

	cb.disconnect(cb.childCMID)
	close(cqStopCh)
	<-cb.cqDone
	C.rdma_destroy_id(cb.childCMID)
	cb.freeBuffers()
	cb.freeQP()
	return ret
}

// --- Persistent server ---
func persistentServerThread(cb *RPingCB) {
	if err := cb.setupQP(cb.childCMID); err != nil {
		fmt.Fprintf(os.Stderr, "setup_qp failed: %v\n", err)
		return
	}

	if err := cb.setupBuffers(); err != nil {
		fmt.Fprintf(os.Stderr, "rping_setup_buffers failed: %v\n", err)
		cb.freeQP()
		return
	}

	if err := cb.postRecv(); err != nil {
		fmt.Fprintf(os.Stderr, "ibv_post_recv failed: %v\n", err)
		cb.freeBuffers()
		cb.freeQP()
		return
	}

	cqStopCh := make(chan struct{})
	cb.cqDone = make(chan struct{})
	go cqThread(cb, cqStopCh)

	if err := cb.accept(); err != nil {
		fmt.Fprintf(os.Stderr, "connect error: %v\n", err)
		close(cqStopCh)
		<-cb.cqDone
		cb.freeBuffers()
		cb.freeQP()
		return
	}

	testServer(cb)
	cb.disconnect(cb.childCMID)
	close(cqStopCh)
	<-cb.cqDone
	cb.freeBuffers()
	cb.freeQP()
	C.rdma_destroy_id(cb.childCMID)
}

func cloneCB(listeningCB *RPingCB) *RPingCB {
	cb := newRPingCB()
	cb.server = true
	cb.mu.Lock()
	cb.state = StateConnectRequest
	cb.mu.Unlock()
	cb.childCMID = listeningCB.childCMID
	cb.sin = listeningCB.sin
	cb.ssource = listeningCB.ssource
	cb.port = listeningCB.port
	cb.verbose = listeningCB.verbose
	cb.selfCreateQP = listeningCB.selfCreateQP
	cb.count = listeningCB.count
	cb.size = listeningCB.size
	cb.validate = listeningCB.validate
	cb.cmChannel = listeningCB.cmChannel
	cb.cmID = listeningCB.cmID
	cb.eventfd = listeningCB.eventfd
	return cb
}

func runPersistentServer(listeningCB *RPingCB) int {
	if err := listeningCB.bindServer(); err != nil {
		fmt.Fprintf(os.Stderr, "bind_server failed: %v\n", err)
		return -1
	}

	for {
		s := listeningCB.waitState(StateConnectRequest, StateError)
		if s != StateConnectRequest {
			fmt.Fprintf(os.Stderr, "wait for CONNECT_REQUEST state %s\n", s)
			return -1
		}

		cb := cloneCB(listeningCB)
		listeningCB.setState(StateIdle)
		go persistentServerThread(cb)
	}
}

// --- Main ---
func main() {
	serverMode := flag.Bool("s", false, "server side")
	clientMode := flag.Bool("c", false, "client side")
	addr := flag.String("a", "", "address")
	srcAddr := flag.String("I", "", "source address to bind to for client")
	port := flag.Int("p", defaultPort, "port")
	size := flag.Int("S", 64, "ping data size")
	count := flag.Int("C", 0, "ping count times (0 = infinite)")
	verbose := flag.Int("v", 0, "display ping data to stdout")
	validate := flag.Bool("V", false, "validate ping data")
	_ = flag.Bool("d", false, "debug printfs")
	persistent := flag.Bool("P", false, "persistent server mode allowing multiple connections")
	selfCreateQP := flag.Bool("q", false, "use self-created, self-modified QP")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "rping -s [-vVd] [-S size] [-C count] [-a addr] [-p port]\n")
		fmt.Fprintf(os.Stderr, "rping -c [-vVd] [-S size] [-C count] [-I addr] -a addr [-p port]\n")
		fmt.Fprintf(os.Stderr, "\t-c\t\tclient side\n")
		fmt.Fprintf(os.Stderr, "\t-I\t\tSource address to bind to for client.\n")
		fmt.Fprintf(os.Stderr, "\t-s\t\tserver side.  To bind to any address with IPv6 use -a ::0\n")
		fmt.Fprintf(os.Stderr, "\t-v\t\tdisplay ping data to stdout\n")
		fmt.Fprintf(os.Stderr, "\t-V\t\tvalidate ping data\n")
		fmt.Fprintf(os.Stderr, "\t-d\t\tdebug printfs\n")
		fmt.Fprintf(os.Stderr, "\t-S size \tping data size\n")
		fmt.Fprintf(os.Stderr, "\t-C count\tping count times\n")
		fmt.Fprintf(os.Stderr, "\t-a addr\t\taddress\n")
		fmt.Fprintf(os.Stderr, "\t-p port\t\tport\n")
		fmt.Fprintf(os.Stderr, "\t-P\t\tpersistent server mode allowing multiple connections\n")
		fmt.Fprintf(os.Stderr, "\t-q\t\tuse self-created, self-modified QP\n")
	}
	flag.Parse()

	if !*serverMode && !*clientMode {
		flag.Usage()
		os.Exit(1)
	}

	if *size < rpingMinBufSize || *size > rpingBufSize-1 {
		fmt.Fprintf(os.Stderr, "Invalid size %d (valid range is %d to %d)\n",
			*size, rpingMinBufSize, rpingBufSize)
		os.Exit(1)
	}

	if *count < 0 {
		fmt.Fprintf(os.Stderr, "Invalid count %d\n", *count)
		os.Exit(1)
	}

	cb := newRPingCB()
	cb.server = *serverMode
	cb.size = *size
	cb.count = *count
	cb.verbose = *verbose
	cb.validate = *validate
	cb.selfCreateQP = *selfCreateQP
	cb.port = uint16(C.go_htobe16(C.ushort(*port)))

	if *addr != "" {
		sin, err := resolveAddr(*addr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to resolve address %s: %v\n", *addr, err)
			os.Exit(1)
		}
		cb.sin = sin
	} else {
		cb.sin.ss_family = C.PF_INET
	}

	if *srcAddr != "" {
		ssource, err := resolveAddr(*srcAddr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to resolve source address %s: %v\n", *srcAddr, err)
			os.Exit(1)
		}
		cb.ssource = ssource
	}

	cb.eventfd = C.eventfd(0, C.EFD_NONBLOCK)
	if cb.eventfd == -1 {
		fmt.Fprintf(os.Stderr, "Could not create event FD\n")
		os.Exit(1)
	}

	var cmChannel *C.struct_rdma_event_channel
	if ret := C.create_event_channel_wrapper(&cmChannel); ret != 0 {
		fmt.Fprintf(os.Stderr, "Failed to create event channel: %v\n", syscall.Errno(ret))
		os.Exit(1)
	}
	cb.cmChannel = cmChannel

	ret := C.rdma_create_id(cb.cmChannel, &cb.cmID, nil, C.RDMA_PS_TCP)
	if ret != 0 {
		fmt.Fprintf(os.Stderr, "rdma_create_id failed\n")
		os.Exit(1)
	}

	cmStopCh := make(chan struct{})
	cb.cmDone = make(chan struct{})
	go cmThread(cb, cmStopCh)

	var result int
	if cb.server {
		if *persistent {
			result = runPersistentServer(cb)
		} else {
			result = runServer(cb)
		}
	} else {
		result = runClient(cb)
	}

	C.rdma_destroy_id(cb.cmID)

	var efdw C.uint64_t = 1
	C.write(cb.eventfd, unsafe.Pointer(&efdw), C.sizeof_uint64_t)

	<-cb.cmDone
	C.rdma_destroy_event_channel(cb.cmChannel)
	C.close(cb.eventfd)

	os.Exit(result)
}

// netAddrFromSin (unused, kept for reference)
func _netAddrFromSin(sin *C.struct_sockaddr_storage) string {
	family := sin.ss_family
	if family == C.AF_INET {
		sin4 := (*C.struct_sockaddr_in)(unsafe.Pointer(sin))
		ip := C.GoBytes(unsafe.Pointer(&sin4.sin_addr), 4)
		port := be16toh(uint16(sin4.sin_port))
		return fmt.Sprintf("%s:%d", net.IP(ip), port)
	} else if family == C.AF_INET6 {
		sin6 := (*C.struct_sockaddr_in6)(unsafe.Pointer(sin))
		ip := C.GoBytes(unsafe.Pointer(&sin6.sin6_addr), 16)
		port := be16toh(uint16(sin6.sin6_port))
		return fmt.Sprintf("[%s]:%d", net.IP(ip), port)
	}
	return "unknown"
}
