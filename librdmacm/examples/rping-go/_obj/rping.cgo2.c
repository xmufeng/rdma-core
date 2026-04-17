
#line 1 "cgo-builtin-prolog"
#include <stddef.h>

/* Define intgo when compiling with GCC.  */
typedef ptrdiff_t intgo;

#define GO_CGO_GOSTRING_TYPEDEF
typedef struct { const char *p; intgo n; } _GoString_;
typedef struct { char *p; intgo n; intgo c; } _GoBytes_;
_GoString_ GoString(char *p);
_GoString_ GoStringN(char *p, int l);
_GoBytes_ GoBytes(void *p, int n);
char *CString(_GoString_);
void *CBytes(_GoBytes_);
void *_CMalloc(size_t);

__attribute__ ((unused))
static size_t _GoStringLen(_GoString_ s) { return (size_t)s.n; }

__attribute__ ((unused))
static const char *_GoStringPtr(_GoString_ s) { return s.p; }

#line 3 "/home/jzbmufeng/workspace/community/rdma-core/librdmacm/examples/rping-go/rping.go"


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

// --- Helper functions to avoid direct errno/field access from Go ---

static int get_channel_fd(struct rdma_event_channel *ch) {
	return ch->fd;
}

static struct ibv_context *get_verbs(struct rdma_cm_id *id) {
	return id->verbs;
}

static uint8_t get_port_num(struct rdma_cm_id *id) {
	return id->port_num;
}

static struct ibv_qp *get_cm_id_qp(struct rdma_cm_id *id) {
	return id->qp;
}

static struct rdma_cm_id *get_event_id(struct rdma_cm_event *event) {
	return event->id;
}

static int get_event_event(struct rdma_cm_event *event) {
	return event->event;
}

static int get_event_status(struct rdma_cm_event *event) {
	return event->status;
}

static uint32_t get_qp_num(struct ibv_qp *qp) {
	return qp->qp_num;
}

static uint32_t get_lkey(struct ibv_mr *mr) {
	return mr->lkey;
}

static uint32_t get_rkey(struct ibv_mr *mr) {
	return mr->rkey;
}

static void fill_rdma_info(struct rping_rdma_info *info, uint64_t buf, uint32_t rkey, uint32_t size) {
	info->buf = htobe64(buf);
	info->rkey = htobe32(rkey);
	info->size = htobe32(size);
}

static uint64_t info_get_buf(struct rping_rdma_info *info) {
	return be64toh(info->buf);
}
static uint32_t info_get_rkey(struct rping_rdma_info *info) {
	return be32toh(info->rkey);
}
static uint32_t info_get_size(struct rping_rdma_info *info) {
	return be32toh(info->size);
}

static void setup_recv_wr(struct ibv_recv_wr *wr, struct ibv_sge *sgl,
                          struct rping_rdma_info *recv_buf, uint32_t lkey) {
	sgl->addr = (uint64_t)(unsigned long)recv_buf;
	sgl->length = sizeof(struct rping_rdma_info);
	sgl->lkey = lkey;
	wr->sg_list = sgl;
	wr->num_sge = 1;
	wr->next = NULL;
}

static void setup_send_wr(struct ibv_send_wr *wr, struct ibv_sge *sgl,
                          struct rping_rdma_info *send_buf, uint32_t lkey) {
	sgl->addr = (uint64_t)(unsigned long)send_buf;
	sgl->length = sizeof(struct rping_rdma_info);
	sgl->lkey = lkey;
	wr->opcode = IBV_WR_SEND;
	wr->send_flags = IBV_SEND_SIGNALED;
	wr->sg_list = sgl;
	wr->num_sge = 1;
	wr->next = NULL;
}

static void setup_rdma_wr(struct ibv_send_wr *wr, struct ibv_sge *sgl,
                          void *rdma_buf, uint32_t lkey) {
	sgl->addr = (uint64_t)(unsigned long)rdma_buf;
	sgl->lkey = lkey;
	wr->send_flags = IBV_SEND_SIGNALED;
	wr->sg_list = sgl;
	wr->num_sge = 1;
	wr->next = NULL;
}

static void set_rdma_read_wr(struct ibv_send_wr *wr, uint32_t rkey,
                             uint64_t remote_addr, uint32_t length) {
	wr->opcode = IBV_WR_RDMA_READ;
	wr->wr.rdma.rkey = rkey;
	wr->wr.rdma.remote_addr = remote_addr;
	wr->sg_list->length = length;
}

static void set_rdma_write_wr(struct ibv_send_wr *wr, uint32_t rkey,
                              uint64_t remote_addr, uint32_t length) {
	wr->opcode = IBV_WR_RDMA_WRITE;
	wr->wr.rdma.rkey = rkey;
	wr->wr.rdma.remote_addr = remote_addr;
	wr->sg_list->length = length;
}

static uint32_t get_rdma_buf_strlen(const char *buf) {
	return (uint32_t)(strlen(buf) + 1);
}

static void set_sockaddr_port(struct sockaddr *addr, uint16_t port) {
	if (addr->sa_family == AF_INET)
		((struct sockaddr_in *)addr)->sin_port = port;
	else
		((struct sockaddr_in6 *)addr)->sin6_port = port;
}

static int resolve_addr(char *dst, struct sockaddr *addr) {
	struct addrinfo *res;
	int ret = getaddrinfo(dst, NULL, NULL, &res);
	if (ret)
		return ret;
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

// Wrapper for rdma_create_event_channel that returns errno
static int create_event_channel_wrapper(struct rdma_event_channel **ch) {
	*ch = rdma_create_event_channel();
	if (!*ch)
		return errno;
	return 0;
}

// Wrapper for ibv_alloc_pd that returns errno
static int alloc_pd_wrapper(struct ibv_context *ctx, struct ibv_pd **pd) {
	*pd = ibv_alloc_pd(ctx);
	if (!*pd)
		return errno;
	return 0;
}

// Wrapper for ibv_create_comp_channel that returns errno
static int create_comp_channel_wrapper(struct ibv_context *ctx, struct ibv_comp_channel **ch) {
	*ch = ibv_create_comp_channel(ctx);
	if (!*ch)
		return errno;
	return 0;
}

// Wrapper for ibv_create_cq that returns errno
static int create_cq_wrapper(struct ibv_context *ctx, int cqe, struct ibv_comp_channel *ch,
                             struct ibv_cq **cq) {
	*cq = ibv_create_cq(ctx, cqe, NULL, ch, 0);
	if (!*cq)
		return errno;
	return 0;
}

// Wrapper for ibv_reg_mr that returns errno
static int reg_mr_wrapper(struct ibv_pd *pd, void *buf, size_t len, int access, struct ibv_mr **mr) {
	*mr = ibv_reg_mr(pd, buf, len, access);
	if (!*mr)
		return errno;
	return 0;
}

// Wrapper for ibv_create_qp that returns errno
static int create_qp_wrapper(struct ibv_pd *pd, struct ibv_qp_init_attr *attr, struct ibv_qp **qp) {
	*qp = ibv_create_qp(pd, attr);
	if (!*qp)
		return errno;
	return 0;
}

// Wrapper for getnameinfo
static void getnameinfo_wrapper(struct sockaddr *addr, socklen_t addrlen,
                                char *host, socklen_t hostlen) {
	getnameinfo(addr, addrlen, host, hostlen, NULL, 0, NI_NUMERICHOST);
}

// Byte order wrappers for Go
static uint16_t go_htobe16(uint16_t v) { return htobe16(v); }
static uint16_t go_be16toh(uint16_t v) { return be16toh(v); }
static uint32_t go_htobe32(uint32_t v) { return htobe32(v); }
static uint32_t go_be32toh(uint32_t v) { return be32toh(v); }
static uint64_t go_htobe64(uint64_t v) { return htobe64(v); }
static uint64_t go_be64toh(uint64_t v) { return be64toh(v); }

// Wrapper for poll with EINTR handling
static int poll_wrapper(struct pollfd *fds, nfds_t nfds, int timeout) {
	int ret;
	do {
		ret = poll(fds, nfds, timeout);
	} while (ret == -1 && errno == EINTR);
	if (ret == -1)
		return -errno;
	return ret;
}

// Wrapper for rdma_get_cm_event
static int get_cm_event_wrapper(struct rdma_event_channel *channel, struct rdma_cm_event **event) {
	int ret = rdma_get_cm_event(channel, event);
	if (ret)
		return -errno;
	return 0;
}

// Wrapper for ibv_get_cq_event
static int get_cq_event_wrapper(struct ibv_comp_channel *channel, struct ibv_cq **cq, void **ctx) {
	int ret = ibv_get_cq_event(channel, cq, ctx);
	if (ret)
		return -errno;
	return 0;
}

#line 1 "cgo-generated-wrapper"


#line 1 "cgo-gcc-prolog"
/*
  If x and y are not equal, the type will be invalid
  (have a negative array count) and an inscrutable error will come
  out of the compiler and hopefully mention "name".
*/
#define __cgo_compile_assert_eq(x, y, name) typedef char name[(x-y)*(x-y)*-2UL+1UL];

/* Check at compile time that the sizes we use match our expectations. */
#define __cgo_size_assert(t, n) __cgo_compile_assert_eq(sizeof(t), (size_t)n, _cgo_sizeof_##t##_is_not_##n)

__cgo_size_assert(char, 1)
__cgo_size_assert(short, 2)
__cgo_size_assert(int, 4)
typedef long long __cgo_long_long;
__cgo_size_assert(__cgo_long_long, 8)
__cgo_size_assert(float, 4)
__cgo_size_assert(double, 8)

extern char* _cgo_topofstack(void);

/*
  We use packed structs, but they are always aligned.
  The pragmas and address-of-packed-member are only recognized as warning
  groups in clang 4.0+, so ignore unknown pragmas first.
*/
#pragma GCC diagnostic ignored "-Wunknown-pragmas"
#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic ignored "-Waddress-of-packed-member"
#pragma GCC diagnostic ignored "-Wunknown-warning-option"
#pragma GCC diagnostic ignored "-Wunaligned-access"

#include <errno.h>
#include <string.h>


#define CGO_NO_SANITIZE_THREAD
#define _cgo_tsan_acquire()
#define _cgo_tsan_release()


#define _cgo_msan_write(addr, sz)

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_alloc_pd_wrapper(void *v)
{
	struct {
		struct ibv_context* p0;
		struct ibv_pd** p1;
		int r;
		char __pad20[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = alloc_pd_wrapper(_cgo_a->p0, _cgo_a->p1);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_close(void *v)
{
	struct {
		int p0;
		char __pad4[4];
		int r;
		char __pad12[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = close(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_create_comp_channel_wrapper(void *v)
{
	struct {
		struct ibv_context* p0;
		struct ibv_comp_channel** p1;
		int r;
		char __pad20[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = create_comp_channel_wrapper(_cgo_a->p0, _cgo_a->p1);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_create_cq_wrapper(void *v)
{
	struct {
		struct ibv_context* p0;
		int p1;
		char __pad12[4];
		struct ibv_comp_channel* p2;
		struct ibv_cq** p3;
		int r;
		char __pad36[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = create_cq_wrapper(_cgo_a->p0, _cgo_a->p1, _cgo_a->p2, _cgo_a->p3);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_create_event_channel_wrapper(void *v)
{
	struct {
		struct rdma_event_channel** p0;
		int r;
		char __pad12[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = create_event_channel_wrapper(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_create_qp_wrapper(void *v)
{
	struct {
		struct ibv_pd* p0;
		struct ibv_qp_init_attr* p1;
		struct ibv_qp** p2;
		int r;
		char __pad28[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = create_qp_wrapper(_cgo_a->p0, _cgo_a->p1, _cgo_a->p2);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_eventfd(void *v)
{
	struct {
		unsigned int p0;
		int p1;
		int r;
		char __pad12[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = eventfd(_cgo_a->p0, _cgo_a->p1);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_fill_rdma_info(void *v)
{
	struct {
		struct rping_rdma_info* p0;
		uint64_t p1;
		uint32_t p2;
		uint32_t p3;
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	_cgo_tsan_acquire();
	fill_rdma_info(_cgo_a->p0, _cgo_a->p1, _cgo_a->p2, _cgo_a->p3);
	_cgo_tsan_release();
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_free(void *v)
{
	struct {
		void* p0;
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	_cgo_tsan_acquire();
	free(_cgo_a->p0);
	_cgo_tsan_release();
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_get_channel_fd(void *v)
{
	struct {
		struct rdma_event_channel* p0;
		int r;
		char __pad12[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = get_channel_fd(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_get_cm_event_wrapper(void *v)
{
	struct {
		struct rdma_event_channel* p0;
		struct rdma_cm_event** p1;
		int r;
		char __pad20[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = get_cm_event_wrapper(_cgo_a->p0, _cgo_a->p1);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_get_cm_id_qp(void *v)
{
	struct {
		struct rdma_cm_id* p0;
		struct ibv_qp* r;
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = (__typeof__(_cgo_a->r)) get_cm_id_qp(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_get_cq_event_wrapper(void *v)
{
	struct {
		struct ibv_comp_channel* p0;
		struct ibv_cq** p1;
		void** p2;
		int r;
		char __pad28[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = get_cq_event_wrapper(_cgo_a->p0, _cgo_a->p1, _cgo_a->p2);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_get_event_event(void *v)
{
	struct {
		struct rdma_cm_event* p0;
		int r;
		char __pad12[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = get_event_event(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_get_event_id(void *v)
{
	struct {
		struct rdma_cm_event* p0;
		struct rdma_cm_id* r;
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = (__typeof__(_cgo_a->r)) get_event_id(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_get_event_status(void *v)
{
	struct {
		struct rdma_cm_event* p0;
		int r;
		char __pad12[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = get_event_status(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_get_lkey(void *v)
{
	struct {
		struct ibv_mr* p0;
		uint32_t r;
		char __pad12[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = get_lkey(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_get_port_num(void *v)
{
	struct {
		struct rdma_cm_id* p0;
		uint8_t r;
		char __pad9[7];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = get_port_num(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_get_qp_num(void *v)
{
	struct {
		struct ibv_qp* p0;
		uint32_t r;
		char __pad12[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = get_qp_num(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_get_rdma_buf_strlen(void *v)
{
	struct {
		char const* p0;
		uint32_t r;
		char __pad12[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = get_rdma_buf_strlen(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_get_rkey(void *v)
{
	struct {
		struct ibv_mr* p0;
		uint32_t r;
		char __pad12[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = get_rkey(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_get_verbs(void *v)
{
	struct {
		struct rdma_cm_id* p0;
		struct ibv_context* r;
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = (__typeof__(_cgo_a->r)) get_verbs(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_getnameinfo_wrapper(void *v)
{
	struct {
		struct sockaddr* p0;
		socklen_t p1;
		char __pad12[4];
		char* p2;
		socklen_t p3;
		char __pad28[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	_cgo_tsan_acquire();
	getnameinfo_wrapper(_cgo_a->p0, _cgo_a->p1, _cgo_a->p2, _cgo_a->p3);
	_cgo_tsan_release();
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_go_be16toh(void *v)
{
	struct {
		uint16_t p0;
		char __pad2[6];
		uint16_t r;
		char __pad10[6];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = go_be16toh(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_go_htobe16(void *v)
{
	struct {
		uint16_t p0;
		char __pad2[6];
		uint16_t r;
		char __pad10[6];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = go_htobe16(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_ibv_ack_cq_events(void *v)
{
	struct {
		struct ibv_cq* p0;
		unsigned int p1;
		char __pad12[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	_cgo_tsan_acquire();
	ibv_ack_cq_events(_cgo_a->p0, _cgo_a->p1);
	_cgo_tsan_release();
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_ibv_dealloc_pd(void *v)
{
	struct {
		struct ibv_pd* p0;
		int r;
		char __pad12[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = ibv_dealloc_pd(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_ibv_dereg_mr(void *v)
{
	struct {
		struct ibv_mr* p0;
		int r;
		char __pad12[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = ibv_dereg_mr(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_ibv_destroy_comp_channel(void *v)
{
	struct {
		struct ibv_comp_channel* p0;
		int r;
		char __pad12[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = ibv_destroy_comp_channel(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_ibv_destroy_cq(void *v)
{
	struct {
		struct ibv_cq* p0;
		int r;
		char __pad12[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = ibv_destroy_cq(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_ibv_destroy_qp(void *v)
{
	struct {
		struct ibv_qp* p0;
		int r;
		char __pad12[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = ibv_destroy_qp(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_ibv_modify_qp(void *v)
{
	struct {
		struct ibv_qp* p0;
		struct ibv_qp_attr* p1;
		int p2;
		char __pad20[4];
		int r;
		char __pad28[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = ibv_modify_qp(_cgo_a->p0, _cgo_a->p1, _cgo_a->p2);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_ibv_poll_cq(void *v)
{
	struct {
		struct ibv_cq* p0;
		int p1;
		char __pad12[4];
		struct ibv_wc* p2;
		int r;
		char __pad28[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = ibv_poll_cq(_cgo_a->p0, _cgo_a->p1, _cgo_a->p2);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_ibv_post_recv(void *v)
{
	struct {
		struct ibv_qp* p0;
		struct ibv_recv_wr* p1;
		struct ibv_recv_wr** p2;
		int r;
		char __pad28[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = ibv_post_recv(_cgo_a->p0, _cgo_a->p1, _cgo_a->p2);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_ibv_post_send(void *v)
{
	struct {
		struct ibv_qp* p0;
		struct ibv_send_wr* p1;
		struct ibv_send_wr** p2;
		int r;
		char __pad28[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = ibv_post_send(_cgo_a->p0, _cgo_a->p1, _cgo_a->p2);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_ibv_req_notify_cq(void *v)
{
	struct {
		struct ibv_cq* p0;
		int p1;
		char __pad12[4];
		int r;
		char __pad20[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = ibv_req_notify_cq(_cgo_a->p0, _cgo_a->p1);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_info_get_buf(void *v)
{
	struct {
		struct rping_rdma_info* p0;
		uint64_t r;
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = info_get_buf(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_info_get_rkey(void *v)
{
	struct {
		struct rping_rdma_info* p0;
		uint32_t r;
		char __pad12[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = info_get_rkey(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_info_get_size(void *v)
{
	struct {
		struct rping_rdma_info* p0;
		uint32_t r;
		char __pad12[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = info_get_size(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_init_conn_param(void *v)
{
	struct {
		struct rdma_conn_param* p0;
		int p1;
		int p2;
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	_cgo_tsan_acquire();
	init_conn_param(_cgo_a->p0, _cgo_a->p1, _cgo_a->p2);
	_cgo_tsan_release();
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_memset(void *v)
{
	struct {
		void* p0;
		int p1;
		char __pad12[4];
		size_t p2;
		void* r;
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = (__typeof__(_cgo_a->r)) memset(_cgo_a->p0, _cgo_a->p1, _cgo_a->p2);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_poll_wrapper(void *v)
{
	struct {
		struct pollfd* p0;
		nfds_t p1;
		int p2;
		char __pad20[4];
		int r;
		char __pad28[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = poll_wrapper(_cgo_a->p0, _cgo_a->p1, _cgo_a->p2);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_rdma_accept(void *v)
{
	struct {
		struct rdma_cm_id* p0;
		struct rdma_conn_param* p1;
		int r;
		char __pad20[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = rdma_accept(_cgo_a->p0, _cgo_a->p1);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_rdma_ack_cm_event(void *v)
{
	struct {
		struct rdma_cm_event* p0;
		int r;
		char __pad12[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = rdma_ack_cm_event(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_rdma_bind_addr(void *v)
{
	struct {
		struct rdma_cm_id* p0;
		struct sockaddr* p1;
		int r;
		char __pad20[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = rdma_bind_addr(_cgo_a->p0, _cgo_a->p1);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_rdma_connect(void *v)
{
	struct {
		struct rdma_cm_id* p0;
		struct rdma_conn_param* p1;
		int r;
		char __pad20[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = rdma_connect(_cgo_a->p0, _cgo_a->p1);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_rdma_create_id(void *v)
{
	struct {
		struct rdma_event_channel* p0;
		struct rdma_cm_id** p1;
		void* p2;
		enum rdma_port_space p3;
		char __pad28[4];
		int r;
		char __pad36[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = rdma_create_id(_cgo_a->p0, _cgo_a->p1, _cgo_a->p2, _cgo_a->p3);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_rdma_create_qp(void *v)
{
	struct {
		struct rdma_cm_id* p0;
		struct ibv_pd* p1;
		struct ibv_qp_init_attr* p2;
		int r;
		char __pad28[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = rdma_create_qp(_cgo_a->p0, _cgo_a->p1, _cgo_a->p2);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_rdma_destroy_event_channel(void *v)
{
	struct {
		struct rdma_event_channel* p0;
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	_cgo_tsan_acquire();
	rdma_destroy_event_channel(_cgo_a->p0);
	_cgo_tsan_release();
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_rdma_destroy_id(void *v)
{
	struct {
		struct rdma_cm_id* p0;
		int r;
		char __pad12[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = rdma_destroy_id(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_rdma_disconnect(void *v)
{
	struct {
		struct rdma_cm_id* p0;
		int r;
		char __pad12[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = rdma_disconnect(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_rdma_establish(void *v)
{
	struct {
		struct rdma_cm_id* p0;
		int r;
		char __pad12[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = rdma_establish(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_rdma_event_str(void *v)
{
	struct {
		enum rdma_cm_event_type p0;
		char __pad4[4];
		char const* r;
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = (__typeof__(_cgo_a->r)) rdma_event_str(_cgo_a->p0);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_rdma_listen(void *v)
{
	struct {
		struct rdma_cm_id* p0;
		int p1;
		char __pad12[4];
		int r;
		char __pad20[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = rdma_listen(_cgo_a->p0, _cgo_a->p1);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_rdma_resolve_addr(void *v)
{
	struct {
		struct rdma_cm_id* p0;
		struct sockaddr* p1;
		struct sockaddr* p2;
		int p3;
		char __pad28[4];
		int r;
		char __pad36[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = rdma_resolve_addr(_cgo_a->p0, _cgo_a->p1, _cgo_a->p2, _cgo_a->p3);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_rdma_resolve_route(void *v)
{
	struct {
		struct rdma_cm_id* p0;
		int p1;
		char __pad12[4];
		int r;
		char __pad20[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = rdma_resolve_route(_cgo_a->p0, _cgo_a->p1);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_reg_mr_wrapper(void *v)
{
	struct {
		struct ibv_pd* p0;
		void* p1;
		size_t p2;
		int p3;
		char __pad28[4];
		struct ibv_mr** p4;
		int r;
		char __pad44[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = reg_mr_wrapper(_cgo_a->p0, _cgo_a->p1, _cgo_a->p2, _cgo_a->p3, _cgo_a->p4);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_resolve_addr(void *v)
{
	struct {
		char* p0;
		struct sockaddr* p1;
		int r;
		char __pad20[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = resolve_addr(_cgo_a->p0, _cgo_a->p1);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_self_modify_qp(void *v)
{
	struct {
		struct ibv_qp* p0;
		struct rdma_cm_id* p1;
		int r;
		char __pad20[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = self_modify_qp(_cgo_a->p0, _cgo_a->p1);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_set_rdma_read_wr(void *v)
{
	struct {
		struct ibv_send_wr* p0;
		uint32_t p1;
		char __pad12[4];
		uint64_t p2;
		uint32_t p3;
		char __pad28[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	_cgo_tsan_acquire();
	set_rdma_read_wr(_cgo_a->p0, _cgo_a->p1, _cgo_a->p2, _cgo_a->p3);
	_cgo_tsan_release();
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_set_rdma_write_wr(void *v)
{
	struct {
		struct ibv_send_wr* p0;
		uint32_t p1;
		char __pad12[4];
		uint64_t p2;
		uint32_t p3;
		char __pad28[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	_cgo_tsan_acquire();
	set_rdma_write_wr(_cgo_a->p0, _cgo_a->p1, _cgo_a->p2, _cgo_a->p3);
	_cgo_tsan_release();
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_set_sockaddr_port(void *v)
{
	struct {
		struct sockaddr* p0;
		uint16_t p1;
		char __pad10[6];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	_cgo_tsan_acquire();
	set_sockaddr_port(_cgo_a->p0, _cgo_a->p1);
	_cgo_tsan_release();
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_setup_rdma_wr(void *v)
{
	struct {
		struct ibv_send_wr* p0;
		struct ibv_sge* p1;
		void* p2;
		uint32_t p3;
		char __pad28[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	_cgo_tsan_acquire();
	setup_rdma_wr(_cgo_a->p0, _cgo_a->p1, _cgo_a->p2, _cgo_a->p3);
	_cgo_tsan_release();
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_setup_recv_wr(void *v)
{
	struct {
		struct ibv_recv_wr* p0;
		struct ibv_sge* p1;
		struct rping_rdma_info* p2;
		uint32_t p3;
		char __pad28[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	_cgo_tsan_acquire();
	setup_recv_wr(_cgo_a->p0, _cgo_a->p1, _cgo_a->p2, _cgo_a->p3);
	_cgo_tsan_release();
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_setup_send_wr(void *v)
{
	struct {
		struct ibv_send_wr* p0;
		struct ibv_sge* p1;
		struct rping_rdma_info* p2;
		uint32_t p3;
		char __pad28[4];
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	_cgo_tsan_acquire();
	setup_send_wr(_cgo_a->p0, _cgo_a->p1, _cgo_a->p2, _cgo_a->p3);
	_cgo_tsan_release();
}

CGO_NO_SANITIZE_THREAD
void
_cgo_f4c9d554b24c_Cfunc_write(void *v)
{
	struct {
		int p0;
		char __pad4[4];
		const void* p1;
		size_t p2;
		ssize_t r;
	} __attribute__((__packed__, __gcc_struct__)) *_cgo_a = v;
	char *_cgo_stktop = _cgo_topofstack();
	__typeof__(_cgo_a->r) _cgo_r;
	_cgo_tsan_acquire();
	_cgo_r = write(_cgo_a->p0, _cgo_a->p1, _cgo_a->p2);
	_cgo_tsan_release();
	_cgo_a = (void*)((char*)_cgo_a + (_cgo_topofstack() - _cgo_stktop));
	_cgo_a->r = _cgo_r;
	_cgo_msan_write(&_cgo_a->r, sizeof(_cgo_a->r));
}

