/* Additional changes for SwiftNIO:
    - prefixed all symbols by 'c_nio_'
*/

#ifndef SWIFTMITM_INCLUDE_LLHTTP_H_
#define SWIFTMITM_INCLUDE_LLHTTP_H_

#define SWIFTMITM_LLHTTP_VERSION_MAJOR 9
#define SWIFTMITM_LLHTTP_VERSION_MINOR 3
#define SWIFTMITM_LLHTTP_VERSION_PATCH 0

#ifndef SWIFTMITM_INCLUDE_LLHTTP_ITSELF_H_
#define SWIFTMITM_INCLUDE_LLHTTP_ITSELF_H_
#ifdef __cplusplus
#include <cstdint>
#else
#include <stdint.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif


typedef struct c_swiftmitm_llhttp__internal_s c_swiftmitm_llhttp__internal_t;
struct c_swiftmitm_llhttp__internal_s {
  int32_t _index;
  void* _span_pos0;
  void* _span_cb0;
  int32_t error;
  const char* reason;
  const char* error_pos;
  void* data;
  void* _current;
  uint64_t content_length;
  uint8_t type;
  uint8_t method;
  uint8_t http_major;
  uint8_t http_minor;
  uint8_t header_state;
  uint16_t lenient_flags;
  uint8_t upgrade;
  uint8_t finish;
  uint16_t flags;
  uint16_t status_code;
  uint8_t initial_message_completed;
  void* settings;
};

int c_swiftmitm_llhttp__internal_init(c_swiftmitm_llhttp__internal_t* s);
int c_swiftmitm_llhttp__internal_execute(c_swiftmitm_llhttp__internal_t* s, const char* p, const char* endp);

#ifdef __cplusplus
}  /* extern "C" */
#endif
#endif  /* INCLUDE_LLHTTP_ITSELF_H_ */


#ifndef SWIFTMITM_LLLLHTTP_C_HEADERS_
#define SWIFTMITM_LLLLHTTP_C_HEADERS_
#ifdef __cplusplus
extern "C" {
#endif

enum swiftmitm_llhttp_errno {
  SWIFTMITM_HPE_OK = 0,
  SWIFTMITM_HPE_INTERNAL = 1,
  SWIFTMITM_HPE_STRICT = 2,
  SWIFTMITM_HPE_CR_EXPECTED = 25,
  SWIFTMITM_HPE_LF_EXPECTED = 3,
  SWIFTMITM_HPE_UNEXPECTED_CONTENT_LENGTH = 4,
  SWIFTMITM_HPE_UNEXPECTED_SPACE = 30,
  SWIFTMITM_HPE_CLOSED_CONNECTION = 5,
  SWIFTMITM_HPE_INVALID_METHOD = 6,
  SWIFTMITM_HPE_INVALID_URL = 7,
  SWIFTMITM_HPE_INVALID_CONSTANT = 8,
  SWIFTMITM_HPE_INVALID_VERSION = 9,
  SWIFTMITM_HPE_INVALID_HEADER_TOKEN = 10,
  SWIFTMITM_HPE_INVALID_CONTENT_LENGTH = 11,
  SWIFTMITM_HPE_INVALID_CHUNK_SIZE = 12,
  SWIFTMITM_HPE_INVALID_STATUS = 13,
  SWIFTMITM_HPE_INVALID_EOF_STATE = 14,
  SWIFTMITM_HPE_INVALID_TRANSFER_ENCODING = 15,
  SWIFTMITM_HPE_CB_MESSAGE_BEGIN = 16,
  SWIFTMITM_HPE_CB_HEADERS_COMPLETE = 17,
  SWIFTMITM_HPE_CB_MESSAGE_COMPLETE = 18,
  SWIFTMITM_HPE_CB_CHUNK_HEADER = 19,
  SWIFTMITM_HPE_CB_CHUNK_COMPLETE = 20,
  SWIFTMITM_HPE_PAUSED = 21,
  SWIFTMITM_HPE_PAUSED_UPGRADE = 22,
  SWIFTMITM_HPE_PAUSED_H2_UPGRADE = 23,
  SWIFTMITM_HPE_USER = 24,
  SWIFTMITM_HPE_CB_URL_COMPLETE = 26,
  SWIFTMITM_HPE_CB_STATUS_COMPLETE = 27,
  SWIFTMITM_HPE_CB_METHOD_COMPLETE = 32,
  SWIFTMITM_HPE_CB_VERSION_COMPLETE = 33,
  SWIFTMITM_HPE_CB_HEADER_FIELD_COMPLETE = 28,
  SWIFTMITM_HPE_CB_HEADER_VALUE_COMPLETE = 29,
  SWIFTMITM_HPE_CB_CHUNK_EXTENSION_NAME_COMPLETE = 34,
  SWIFTMITM_HPE_CB_CHUNK_EXTENSION_VALUE_COMPLETE = 35,
  SWIFTMITM_HPE_CB_RESET = 31,
  SWIFTMITM_HPE_CB_PROTOCOL_COMPLETE = 38
};
typedef enum swiftmitm_llhttp_errno swiftmitm_llhttp_errno_t;

enum swiftmitm_llhttp_flags {
  SWIFTMITM_F_CONNECTION_KEEP_ALIVE = 0x1,
  SWIFTMITM_F_CONNECTION_CLOSE = 0x2,
  SWIFTMITM_F_CONNECTION_UPGRADE = 0x4,
  SWIFTMITM_F_CHUNKED = 0x8,
  SWIFTMITM_F_UPGRADE = 0x10,
  SWIFTMITM_F_CONTENT_LENGTH = 0x20,
  SWIFTMITM_F_SKIPBODY = 0x40,
  SWIFTMITM_F_TRAILING = 0x80,
  SWIFTMITM_F_TRANSFER_ENCODING = 0x200
};
typedef enum swiftmitm_llhttp_flags swiftmitm_llhttp_flags_t;

enum swiftmitm_llhttp_lenient_flags {
  SWIFTMITM_LENIENT_HEADERS = 0x1,
  SWIFTMITM_LENIENT_CHUNKED_LENGTH = 0x2,
  SWIFTMITM_LENIENT_KEEP_ALIVE = 0x4,
  SWIFTMITM_LENIENT_TRANSFER_ENCODING = 0x8,
  SWIFTMITM_LENIENT_VERSION = 0x10,
  SWIFTMITM_LENIENT_DATA_AFTER_CLOSE = 0x20,
  SWIFTMITM_LENIENT_OPTIONAL_LF_AFTER_CR = 0x40,
  SWIFTMITM_LENIENT_OPTIONAL_CRLF_AFTER_CHUNK = 0x80,
  SWIFTMITM_LENIENT_OPTIONAL_CR_BEFORE_LF = 0x100,
  SWIFTMITM_LENIENT_SPACES_AFTER_CHUNK_SIZE = 0x200
};
typedef enum swiftmitm_llhttp_lenient_flags swiftmitm_llhttp_lenient_flags_t;

enum swiftmitm_llhttp_type {
  SWIFTMITM_HTTP_BOTH = 0,
  SWIFTMITM_HTTP_REQUEST = 1,
  SWIFTMITM_HTTP_RESPONSE = 2
};
typedef enum swiftmitm_llhttp_type swiftmitm_llhttp_type_t;

enum c_swiftmitm_llhttp_finish {
  SWIFTMITM_HTTP_FINISH_SAFE = 0,
  SWIFTMITM_HTTP_FINISH_SAFE_WITH_CB = 1,
  SWIFTMITM_HTTP_FINISH_UNSAFE = 2
};
typedef enum c_swiftmitm_llhttp_finish c_swiftmitm_llhttp_finish_t;

enum swiftmitm_llhttp_method {
  SWIFTMITM_HTTP_DELETE = 0,
  SWIFTMITM_HTTP_GET = 1,
  SWIFTMITM_HTTP_HEAD = 2,
  SWIFTMITM_HTTP_POST = 3,
  SWIFTMITM_HTTP_PUT = 4,
  SWIFTMITM_HTTP_CONNECT = 5,
  SWIFTMITM_HTTP_OPTIONS = 6,
  SWIFTMITM_HTTP_TRACE = 7,
  SWIFTMITM_HTTP_COPY = 8,
  SWIFTMITM_HTTP_LOCK = 9,
  SWIFTMITM_HTTP_MKCOL = 10,
  SWIFTMITM_HTTP_MOVE = 11,
  SWIFTMITM_HTTP_PROPFIND = 12,
  SWIFTMITM_HTTP_PROPPATCH = 13,
  SWIFTMITM_HTTP_SEARCH = 14,
  SWIFTMITM_HTTP_UNLOCK = 15,
  SWIFTMITM_HTTP_BIND = 16,
  SWIFTMITM_HTTP_REBIND = 17,
  SWIFTMITM_HTTP_UNBIND = 18,
  SWIFTMITM_HTTP_ACL = 19,
  SWIFTMITM_HTTP_REPORT = 20,
  SWIFTMITM_HTTP_MKACTIVITY = 21,
  SWIFTMITM_HTTP_CHECKOUT = 22,
  SWIFTMITM_HTTP_MERGE = 23,
  SWIFTMITM_HTTP_MSEARCH = 24,
  SWIFTMITM_HTTP_NOTIFY = 25,
  SWIFTMITM_HTTP_SUBSCRIBE = 26,
  SWIFTMITM_HTTP_UNSUBSCRIBE = 27,
  SWIFTMITM_HTTP_PATCH = 28,
  SWIFTMITM_HTTP_PURGE = 29,
  SWIFTMITM_HTTP_MKCALENDAR = 30,
  SWIFTMITM_HTTP_LINK = 31,
  SWIFTMITM_HTTP_UNLINK = 32,
  SWIFTMITM_HTTP_SOURCE = 33,
  SWIFTMITM_HTTP_PRI = 34,
  SWIFTMITM_HTTP_DESCRIBE = 35,
  SWIFTMITM_HTTP_ANNOUNCE = 36,
  SWIFTMITM_HTTP_SETUP = 37,
  SWIFTMITM_HTTP_PLAY = 38,
  SWIFTMITM_HTTP_PAUSE = 39,
  SWIFTMITM_HTTP_TEARDOWN = 40,
  SWIFTMITM_HTTP_GET_PARAMETER = 41,
  SWIFTMITM_HTTP_SET_PARAMETER = 42,
  SWIFTMITM_HTTP_REDIRECT = 43,
  SWIFTMITM_HTTP_RECORD = 44,
  SWIFTMITM_HTTP_FLUSH = 45,
  SWIFTMITM_HTTP_QUERY = 46
};
typedef enum swiftmitm_llhttp_method swiftmitm_llhttp_method_t;

enum swiftmitm_llhttp_status {
  SWIFTMITM_HTTP_STATUS_CONTINUE = 100,
  SWIFTMITM_HTTP_STATUS_SWITCHING_PROTOCOLS = 101,
  SWIFTMITM_HTTP_STATUS_PROCESSING = 102,
  SWIFTMITM_HTTP_STATUS_EARLY_HINTS = 103,
  SWIFTMITM_HTTP_STATUS_RESPONSE_IS_STALE = 110,
  SWIFTMITM_HTTP_STATUS_REVALIDATION_FAILED = 111,
  SWIFTMITM_HTTP_STATUS_DISCONNECTED_OPERATION = 112,
  SWIFTMITM_HTTP_STATUS_HEURISTIC_EXPIRATION = 113,
  SWIFTMITM_HTTP_STATUS_MISCELLANEOUS_WARNING = 199,
  SWIFTMITM_HTTP_STATUS_OK = 200,
  SWIFTMITM_HTTP_STATUS_CREATED = 201,
  SWIFTMITM_HTTP_STATUS_ACCEPTED = 202,
  SWIFTMITM_HTTP_STATUS_NON_AUTHORITATIVE_INFORMATION = 203,
  SWIFTMITM_HTTP_STATUS_NO_CONTENT = 204,
  SWIFTMITM_HTTP_STATUS_RESET_CONTENT = 205,
  SWIFTMITM_HTTP_STATUS_PARTIAL_CONTENT = 206,
  SWIFTMITM_HTTP_STATUS_MULTI_STATUS = 207,
  SWIFTMITM_HTTP_STATUS_ALREADY_REPORTED = 208,
  SWIFTMITM_HTTP_STATUS_TRANSFORMATION_APPLIED = 214,
  SWIFTMITM_HTTP_STATUS_IM_USED = 226,
  SWIFTMITM_HTTP_STATUS_MISCELLANEOUS_PERSISTENT_WARNING = 299,
  SWIFTMITM_HTTP_STATUS_MULTIPLE_CHOICES = 300,
  SWIFTMITM_HTTP_STATUS_MOVED_PERMANENTLY = 301,
  SWIFTMITM_HTTP_STATUS_FOUND = 302,
  SWIFTMITM_HTTP_STATUS_SEE_OTHER = 303,
  SWIFTMITM_HTTP_STATUS_NOT_MODIFIED = 304,
  SWIFTMITM_HTTP_STATUS_USE_PROXY = 305,
  SWIFTMITM_HTTP_STATUS_SWITCH_PROXY = 306,
  SWIFTMITM_HTTP_STATUS_TEMPORARY_REDIRECT = 307,
  SWIFTMITM_HTTP_STATUS_PERMANENT_REDIRECT = 308,
  SWIFTMITM_HTTP_STATUS_BAD_REQUEST = 400,
  SWIFTMITM_HTTP_STATUS_UNAUTHORIZED = 401,
  SWIFTMITM_HTTP_STATUS_PAYMENT_REQUIRED = 402,
  SWIFTMITM_HTTP_STATUS_FORBIDDEN = 403,
  SWIFTMITM_HTTP_STATUS_NOT_FOUND = 404,
  SWIFTMITM_HTTP_STATUS_METHOD_NOT_ALLOWED = 405,
  SWIFTMITM_HTTP_STATUS_NOT_ACCEPTABLE = 406,
  SWIFTMITM_HTTP_STATUS_PROXY_AUTHENTICATION_REQUIRED = 407,
  SWIFTMITM_HTTP_STATUS_REQUEST_TIMEOUT = 408,
  SWIFTMITM_HTTP_STATUS_CONFLICT = 409,
  SWIFTMITM_HTTP_STATUS_GONE = 410,
  SWIFTMITM_HTTP_STATUS_LENGTH_REQUIRED = 411,
  SWIFTMITM_HTTP_STATUS_PRECONDITION_FAILED = 412,
  SWIFTMITM_HTTP_STATUS_PAYLOAD_TOO_LARGE = 413,
  SWIFTMITM_HTTP_STATUS_URI_TOO_LONG = 414,
  SWIFTMITM_HTTP_STATUS_UNSUPPORTED_MEDIA_TYPE = 415,
  SWIFTMITM_HTTP_STATUS_RANGE_NOT_SATISFIABLE = 416,
  SWIFTMITM_HTTP_STATUS_EXPECTATION_FAILED = 417,
  SWIFTMITM_HTTP_STATUS_IM_A_TEAPOT = 418,
  SWIFTMITM_HTTP_STATUS_PAGE_EXPIRED = 419,
  SWIFTMITM_HTTP_STATUS_ENHANCE_YOUR_CALM = 420,
  SWIFTMITM_HTTP_STATUS_MISDIRECTED_REQUEST = 421,
  SWIFTMITM_HTTP_STATUS_UNPROCESSABLE_ENTITY = 422,
  SWIFTMITM_HTTP_STATUS_LOCKED = 423,
  SWIFTMITM_HTTP_STATUS_FAILED_DEPENDENCY = 424,
  SWIFTMITM_HTTP_STATUS_TOO_EARLY = 425,
  SWIFTMITM_HTTP_STATUS_UPGRADE_REQUIRED = 426,
  SWIFTMITM_HTTP_STATUS_PRECONDITION_REQUIRED = 428,
  SWIFTMITM_HTTP_STATUS_TOO_MANY_REQUESTS = 429,
  SWIFTMITM_HTTP_STATUS_REQUEST_HEADER_FIELDS_TOO_LARGE_UNOFFICIAL = 430,
  SWIFTMITM_HTTP_STATUS_REQUEST_HEADER_FIELDS_TOO_LARGE = 431,
  SWIFTMITM_HTTP_STATUS_LOGIN_TIMEOUT = 440,
  SWIFTMITM_HTTP_STATUS_NO_RESPONSE = 444,
  SWIFTMITM_HTTP_STATUS_RETRY_WITH = 449,
  SWIFTMITM_HTTP_STATUS_BLOCKED_BY_PARENTAL_CONTROL = 450,
  SWIFTMITM_HTTP_STATUS_UNAVAILABLE_FOR_LEGAL_REASONS = 451,
  SWIFTMITM_HTTP_STATUS_CLIENT_CLOSED_LOAD_BALANCED_REQUEST = 460,
  SWIFTMITM_HTTP_STATUS_INVALID_X_FORWARDED_FOR = 463,
  SWIFTMITM_HTTP_STATUS_REQUEST_HEADER_TOO_LARGE = 494,
  SWIFTMITM_HTTP_STATUS_SSL_CERTIFICATE_ERROR = 495,
  SWIFTMITM_HTTP_STATUS_SSL_CERTIFICATE_REQUIRED = 496,
  SWIFTMITM_HTTP_STATUS_HTTP_REQUEST_SENT_TO_HTTPS_PORT = 497,
  SWIFTMITM_HTTP_STATUS_INVALID_TOKEN = 498,
  SWIFTMITM_HTTP_STATUS_CLIENT_CLOSED_REQUEST = 499,
  SWIFTMITM_HTTP_STATUS_INTERNAL_SERVER_ERROR = 500,
  SWIFTMITM_HTTP_STATUS_NOT_IMPLEMENTED = 501,
  SWIFTMITM_HTTP_STATUS_BAD_GATEWAY = 502,
  SWIFTMITM_HTTP_STATUS_SERVICE_UNAVAILABLE = 503,
  SWIFTMITM_HTTP_STATUS_GATEWAY_TIMEOUT = 504,
  SWIFTMITM_HTTP_STATUS_HTTP_VERSION_NOT_SUPPORTED = 505,
  SWIFTMITM_HTTP_STATUS_VARIANT_ALSO_NEGOTIATES = 506,
  SWIFTMITM_HTTP_STATUS_INSUFFICIENT_STORAGE = 507,
  SWIFTMITM_HTTP_STATUS_LOOP_DETECTED = 508,
  SWIFTMITM_HTTP_STATUS_BANDWIDTH_LIMIT_EXCEEDED = 509,
  SWIFTMITM_HTTP_STATUS_NOT_EXTENDED = 510,
  SWIFTMITM_HTTP_STATUS_NETWORK_AUTHENTICATION_REQUIRED = 511,
  SWIFTMITM_HTTP_STATUS_WEB_SERVER_UNKNOWN_ERROR = 520,
  SWIFTMITM_HTTP_STATUS_WEB_SERVER_IS_DOWN = 521,
  SWIFTMITM_HTTP_STATUS_CONNECTION_TIMEOUT = 522,
  SWIFTMITM_HTTP_STATUS_ORIGIN_IS_UNREACHABLE = 523,
  SWIFTMITM_HTTP_STATUS_TIMEOUT_OCCURED = 524,
  SWIFTMITM_HTTP_STATUS_SSL_HANDSHAKE_FAILED = 525,
  SWIFTMITM_HTTP_STATUS_INVALID_SSL_CERTIFICATE = 526,
  SWIFTMITM_HTTP_STATUS_RAILGUN_ERROR = 527,
  SWIFTMITM_HTTP_STATUS_SITE_IS_OVERLOADED = 529,
  SWIFTMITM_HTTP_STATUS_SITE_IS_FROZEN = 530,
  SWIFTMITM_HTTP_STATUS_IDENTITY_PROVIDER_AUTHENTICATION_ERROR = 561,
  SWIFTMITM_HTTP_STATUS_NETWORK_READ_TIMEOUT = 598,
  SWIFTMITM_HTTP_STATUS_NETWORK_CONNECT_TIMEOUT = 599
};
typedef enum swiftmitm_llhttp_status swiftmitm_llhttp_status_t;

#define SWIFTMITM_HTTP_ERRNO_MAP(XX) \
  XX(0, OK, OK) \
  XX(1, INTERNAL, INTERNAL) \
  XX(2, STRICT, STRICT) \
  XX(25, CR_EXPECTED, CR_EXPECTED) \
  XX(3, LF_EXPECTED, LF_EXPECTED) \
  XX(4, UNEXPECTED_CONTENT_LENGTH, UNEXPECTED_CONTENT_LENGTH) \
  XX(30, UNEXPECTED_SPACE, UNEXPECTED_SPACE) \
  XX(5, CLOSED_CONNECTION, CLOSED_CONNECTION) \
  XX(6, INVALID_METHOD, INVALID_METHOD) \
  XX(7, INVALID_URL, INVALID_URL) \
  XX(8, INVALID_CONSTANT, INVALID_CONSTANT) \
  XX(9, INVALID_VERSION, INVALID_VERSION) \
  XX(10, INVALID_HEADER_TOKEN, INVALID_HEADER_TOKEN) \
  XX(11, INVALID_CONTENT_LENGTH, INVALID_CONTENT_LENGTH) \
  XX(12, INVALID_CHUNK_SIZE, INVALID_CHUNK_SIZE) \
  XX(13, INVALID_STATUS, INVALID_STATUS) \
  XX(14, INVALID_EOF_STATE, INVALID_EOF_STATE) \
  XX(15, INVALID_TRANSFER_ENCODING, INVALID_TRANSFER_ENCODING) \
  XX(16, CB_MESSAGE_BEGIN, CB_MESSAGE_BEGIN) \
  XX(17, CB_HEADERS_COMPLETE, CB_HEADERS_COMPLETE) \
  XX(18, CB_MESSAGE_COMPLETE, CB_MESSAGE_COMPLETE) \
  XX(19, CB_CHUNK_HEADER, CB_CHUNK_HEADER) \
  XX(20, CB_CHUNK_COMPLETE, CB_CHUNK_COMPLETE) \
  XX(21, PAUSED, PAUSED) \
  XX(22, PAUSED_UPGRADE, PAUSED_UPGRADE) \
  XX(23, PAUSED_H2_UPGRADE, PAUSED_H2_UPGRADE) \
  XX(24, USER, USER) \
  XX(26, CB_URL_COMPLETE, CB_URL_COMPLETE) \
  XX(27, CB_STATUS_COMPLETE, CB_STATUS_COMPLETE) \
  XX(32, CB_METHOD_COMPLETE, CB_METHOD_COMPLETE) \
  XX(33, CB_VERSION_COMPLETE, CB_VERSION_COMPLETE) \
  XX(28, CB_HEADER_FIELD_COMPLETE, CB_HEADER_FIELD_COMPLETE) \
  XX(29, CB_HEADER_VALUE_COMPLETE, CB_HEADER_VALUE_COMPLETE) \
  XX(34, CB_CHUNK_EXTENSION_NAME_COMPLETE, CB_CHUNK_EXTENSION_NAME_COMPLETE) \
  XX(35, CB_CHUNK_EXTENSION_VALUE_COMPLETE, CB_CHUNK_EXTENSION_VALUE_COMPLETE) \
  XX(31, CB_RESET, CB_RESET) \
  XX(38, CB_PROTOCOL_COMPLETE, CB_PROTOCOL_COMPLETE) \


#define SWIFTMITM_HTTP_METHOD_MAP(XX) \
  XX(0, DELETE, DELETE) \
  XX(1, GET, GET) \
  XX(2, HEAD, HEAD) \
  XX(3, POST, POST) \
  XX(4, PUT, PUT) \
  XX(5, CONNECT, CONNECT) \
  XX(6, OPTIONS, OPTIONS) \
  XX(7, TRACE, TRACE) \
  XX(8, COPY, COPY) \
  XX(9, LOCK, LOCK) \
  XX(10, MKCOL, MKCOL) \
  XX(11, MOVE, MOVE) \
  XX(12, PROPFIND, PROPFIND) \
  XX(13, PROPPATCH, PROPPATCH) \
  XX(14, SEARCH, SEARCH) \
  XX(15, UNLOCK, UNLOCK) \
  XX(16, BIND, BIND) \
  XX(17, REBIND, REBIND) \
  XX(18, UNBIND, UNBIND) \
  XX(19, ACL, ACL) \
  XX(20, REPORT, REPORT) \
  XX(21, MKACTIVITY, MKACTIVITY) \
  XX(22, CHECKOUT, CHECKOUT) \
  XX(23, MERGE, MERGE) \
  XX(24, MSEARCH, M-SEARCH) \
  XX(25, NOTIFY, NOTIFY) \
  XX(26, SUBSCRIBE, SUBSCRIBE) \
  XX(27, UNSUBSCRIBE, UNSUBSCRIBE) \
  XX(28, PATCH, PATCH) \
  XX(29, PURGE, PURGE) \
  XX(30, MKCALENDAR, MKCALENDAR) \
  XX(31, LINK, LINK) \
  XX(32, UNLINK, UNLINK) \
  XX(33, SOURCE, SOURCE) \
  XX(46, QUERY, QUERY) \


#define RTSP_METHOD_MAP(XX) \
  XX(1, GET, GET) \
  XX(3, POST, POST) \
  XX(6, OPTIONS, OPTIONS) \
  XX(35, DESCRIBE, DESCRIBE) \
  XX(36, ANNOUNCE, ANNOUNCE) \
  XX(37, SETUP, SETUP) \
  XX(38, PLAY, PLAY) \
  XX(39, PAUSE, PAUSE) \
  XX(40, TEARDOWN, TEARDOWN) \
  XX(41, GET_PARAMETER, GET_PARAMETER) \
  XX(42, SET_PARAMETER, SET_PARAMETER) \
  XX(43, REDIRECT, REDIRECT) \
  XX(44, RECORD, RECORD) \
  XX(45, FLUSH, FLUSH) \


#define SWIFTMITM_HTTP_ALL_METHOD_MAP(XX) \
  XX(0, DELETE, DELETE) \
  XX(1, GET, GET) \
  XX(2, HEAD, HEAD) \
  XX(3, POST, POST) \
  XX(4, PUT, PUT) \
  XX(5, CONNECT, CONNECT) \
  XX(6, OPTIONS, OPTIONS) \
  XX(7, TRACE, TRACE) \
  XX(8, COPY, COPY) \
  XX(9, LOCK, LOCK) \
  XX(10, MKCOL, MKCOL) \
  XX(11, MOVE, MOVE) \
  XX(12, PROPFIND, PROPFIND) \
  XX(13, PROPPATCH, PROPPATCH) \
  XX(14, SEARCH, SEARCH) \
  XX(15, UNLOCK, UNLOCK) \
  XX(16, BIND, BIND) \
  XX(17, REBIND, REBIND) \
  XX(18, UNBIND, UNBIND) \
  XX(19, ACL, ACL) \
  XX(20, REPORT, REPORT) \
  XX(21, MKACTIVITY, MKACTIVITY) \
  XX(22, CHECKOUT, CHECKOUT) \
  XX(23, MERGE, MERGE) \
  XX(24, MSEARCH, M-SEARCH) \
  XX(25, NOTIFY, NOTIFY) \
  XX(26, SUBSCRIBE, SUBSCRIBE) \
  XX(27, UNSUBSCRIBE, UNSUBSCRIBE) \
  XX(28, PATCH, PATCH) \
  XX(29, PURGE, PURGE) \
  XX(30, MKCALENDAR, MKCALENDAR) \
  XX(31, LINK, LINK) \
  XX(32, UNLINK, UNLINK) \
  XX(33, SOURCE, SOURCE) \
  XX(34, PRI, PRI) \
  XX(35, DESCRIBE, DESCRIBE) \
  XX(36, ANNOUNCE, ANNOUNCE) \
  XX(37, SETUP, SETUP) \
  XX(38, PLAY, PLAY) \
  XX(39, PAUSE, PAUSE) \
  XX(40, TEARDOWN, TEARDOWN) \
  XX(41, GET_PARAMETER, GET_PARAMETER) \
  XX(42, SET_PARAMETER, SET_PARAMETER) \
  XX(43, REDIRECT, REDIRECT) \
  XX(44, RECORD, RECORD) \
  XX(45, FLUSH, FLUSH) \
  XX(46, QUERY, QUERY) \


#define SWIFTMITM_HTTP_STATUS_MAP(XX) \
  XX(100, CONTINUE, CONTINUE) \
  XX(101, SWITCHING_PROTOCOLS, SWITCHING_PROTOCOLS) \
  XX(102, PROCESSING, PROCESSING) \
  XX(103, EARLY_HINTS, EARLY_HINTS) \
  XX(110, RESPONSE_IS_STALE, RESPONSE_IS_STALE) \
  XX(111, REVALIDATION_FAILED, REVALIDATION_FAILED) \
  XX(112, DISCONNECTED_OPERATION, DISCONNECTED_OPERATION) \
  XX(113, HEURISTIC_EXPIRATION, HEURISTIC_EXPIRATION) \
  XX(199, MISCELLANEOUS_WARNING, MISCELLANEOUS_WARNING) \
  XX(200, OK, OK) \
  XX(201, CREATED, CREATED) \
  XX(202, ACCEPTED, ACCEPTED) \
  XX(203, NON_AUTHORITATIVE_INFORMATION, NON_AUTHORITATIVE_INFORMATION) \
  XX(204, NO_CONTENT, NO_CONTENT) \
  XX(205, RESET_CONTENT, RESET_CONTENT) \
  XX(206, PARTIAL_CONTENT, PARTIAL_CONTENT) \
  XX(207, MULTI_STATUS, MULTI_STATUS) \
  XX(208, ALREADY_REPORTED, ALREADY_REPORTED) \
  XX(214, TRANSFORMATION_APPLIED, TRANSFORMATION_APPLIED) \
  XX(226, IM_USED, IM_USED) \
  XX(299, MISCELLANEOUS_PERSISTENT_WARNING, MISCELLANEOUS_PERSISTENT_WARNING) \
  XX(300, MULTIPLE_CHOICES, MULTIPLE_CHOICES) \
  XX(301, MOVED_PERMANENTLY, MOVED_PERMANENTLY) \
  XX(302, FOUND, FOUND) \
  XX(303, SEE_OTHER, SEE_OTHER) \
  XX(304, NOT_MODIFIED, NOT_MODIFIED) \
  XX(305, USE_PROXY, USE_PROXY) \
  XX(306, SWITCH_PROXY, SWITCH_PROXY) \
  XX(307, TEMPORARY_REDIRECT, TEMPORARY_REDIRECT) \
  XX(308, PERMANENT_REDIRECT, PERMANENT_REDIRECT) \
  XX(400, BAD_REQUEST, BAD_REQUEST) \
  XX(401, UNAUTHORIZED, UNAUTHORIZED) \
  XX(402, PAYMENT_REQUIRED, PAYMENT_REQUIRED) \
  XX(403, FORBIDDEN, FORBIDDEN) \
  XX(404, NOT_FOUND, NOT_FOUND) \
  XX(405, METHOD_NOT_ALLOWED, METHOD_NOT_ALLOWED) \
  XX(406, NOT_ACCEPTABLE, NOT_ACCEPTABLE) \
  XX(407, PROXY_AUTHENTICATION_REQUIRED, PROXY_AUTHENTICATION_REQUIRED) \
  XX(408, REQUEST_TIMEOUT, REQUEST_TIMEOUT) \
  XX(409, CONFLICT, CONFLICT) \
  XX(410, GONE, GONE) \
  XX(411, LENGTH_REQUIRED, LENGTH_REQUIRED) \
  XX(412, PRECONDITION_FAILED, PRECONDITION_FAILED) \
  XX(413, PAYLOAD_TOO_LARGE, PAYLOAD_TOO_LARGE) \
  XX(414, URI_TOO_LONG, URI_TOO_LONG) \
  XX(415, UNSUPPORTED_MEDIA_TYPE, UNSUPPORTED_MEDIA_TYPE) \
  XX(416, RANGE_NOT_SATISFIABLE, RANGE_NOT_SATISFIABLE) \
  XX(417, EXPECTATION_FAILED, EXPECTATION_FAILED) \
  XX(418, IM_A_TEAPOT, IM_A_TEAPOT) \
  XX(419, PAGE_EXPIRED, PAGE_EXPIRED) \
  XX(420, ENHANCE_YOUR_CALM, ENHANCE_YOUR_CALM) \
  XX(421, MISDIRECTED_REQUEST, MISDIRECTED_REQUEST) \
  XX(422, UNPROCESSABLE_ENTITY, UNPROCESSABLE_ENTITY) \
  XX(423, LOCKED, LOCKED) \
  XX(424, FAILED_DEPENDENCY, FAILED_DEPENDENCY) \
  XX(425, TOO_EARLY, TOO_EARLY) \
  XX(426, UPGRADE_REQUIRED, UPGRADE_REQUIRED) \
  XX(428, PRECONDITION_REQUIRED, PRECONDITION_REQUIRED) \
  XX(429, TOO_MANY_REQUESTS, TOO_MANY_REQUESTS) \
  XX(430, REQUEST_HEADER_FIELDS_TOO_LARGE_UNOFFICIAL, REQUEST_HEADER_FIELDS_TOO_LARGE_UNOFFICIAL) \
  XX(431, REQUEST_HEADER_FIELDS_TOO_LARGE, REQUEST_HEADER_FIELDS_TOO_LARGE) \
  XX(440, LOGIN_TIMEOUT, LOGIN_TIMEOUT) \
  XX(444, NO_RESPONSE, NO_RESPONSE) \
  XX(449, RETRY_WITH, RETRY_WITH) \
  XX(450, BLOCKED_BY_PARENTAL_CONTROL, BLOCKED_BY_PARENTAL_CONTROL) \
  XX(451, UNAVAILABLE_FOR_LEGAL_REASONS, UNAVAILABLE_FOR_LEGAL_REASONS) \
  XX(460, CLIENT_CLOSED_LOAD_BALANCED_REQUEST, CLIENT_CLOSED_LOAD_BALANCED_REQUEST) \
  XX(463, INVALID_X_FORWARDED_FOR, INVALID_X_FORWARDED_FOR) \
  XX(494, REQUEST_HEADER_TOO_LARGE, REQUEST_HEADER_TOO_LARGE) \
  XX(495, SSL_CERTIFICATE_ERROR, SSL_CERTIFICATE_ERROR) \
  XX(496, SSL_CERTIFICATE_REQUIRED, SSL_CERTIFICATE_REQUIRED) \
  XX(497, SWIFTMITM_HTTP_REQUEST_SENT_TO_HTTPS_PORT, SWIFTMITM_HTTP_REQUEST_SENT_TO_HTTPS_PORT) \
  XX(498, INVALID_TOKEN, INVALID_TOKEN) \
  XX(499, CLIENT_CLOSED_REQUEST, CLIENT_CLOSED_REQUEST) \
  XX(500, INTERNAL_SERVER_ERROR, INTERNAL_SERVER_ERROR) \
  XX(501, NOT_IMPLEMENTED, NOT_IMPLEMENTED) \
  XX(502, BAD_GATEWAY, BAD_GATEWAY) \
  XX(503, SERVICE_UNAVAILABLE, SERVICE_UNAVAILABLE) \
  XX(504, GATEWAY_TIMEOUT, GATEWAY_TIMEOUT) \
  XX(505, SWIFTMITM_HTTP_VERSION_NOT_SUPPORTED, SWIFTMITM_HTTP_VERSION_NOT_SUPPORTED) \
  XX(506, VARIANT_ALSO_NEGOTIATES, VARIANT_ALSO_NEGOTIATES) \
  XX(507, INSUFFICIENT_STORAGE, INSUFFICIENT_STORAGE) \
  XX(508, LOOP_DETECTED, LOOP_DETECTED) \
  XX(509, BANDWIDTH_LIMIT_EXCEEDED, BANDWIDTH_LIMIT_EXCEEDED) \
  XX(510, NOT_EXTENDED, NOT_EXTENDED) \
  XX(511, NETWORK_AUTHENTICATION_REQUIRED, NETWORK_AUTHENTICATION_REQUIRED) \
  XX(520, WEB_SERVER_UNKNOWN_ERROR, WEB_SERVER_UNKNOWN_ERROR) \
  XX(521, WEB_SERVER_IS_DOWN, WEB_SERVER_IS_DOWN) \
  XX(522, CONNECTION_TIMEOUT, CONNECTION_TIMEOUT) \
  XX(523, ORIGIN_IS_UNREACHABLE, ORIGIN_IS_UNREACHABLE) \
  XX(524, TIMEOUT_OCCURED, TIMEOUT_OCCURED) \
  XX(525, SSL_HANDSHAKE_FAILED, SSL_HANDSHAKE_FAILED) \
  XX(526, INVALID_SSL_CERTIFICATE, INVALID_SSL_CERTIFICATE) \
  XX(527, RAILGUN_ERROR, RAILGUN_ERROR) \
  XX(529, SITE_IS_OVERLOADED, SITE_IS_OVERLOADED) \
  XX(530, SITE_IS_FROZEN, SITE_IS_FROZEN) \
  XX(561, IDENTITY_PROVIDER_AUTHENTICATION_ERROR, IDENTITY_PROVIDER_AUTHENTICATION_ERROR) \
  XX(598, NETWORK_READ_TIMEOUT, NETWORK_READ_TIMEOUT) \
  XX(599, NETWORK_CONNECT_TIMEOUT, NETWORK_CONNECT_TIMEOUT) \


#ifdef __cplusplus
}  /* extern "C" */
#endif
#endif  /* LLLLHTTP_C_HEADERS_ */


#ifndef SWIFTMITM_INCLUDE_LLHTTP_API_H_
#define SWIFTMITM_INCLUDE_LLHTTP_API_H_
#include <stddef.h>
#ifdef __cplusplus
extern "C" {
#endif

#if defined(__wasm__)
#define SWIFTMITM_LLHTTP_EXPORT __attribute__((visibility("default")))
#elif defined(_WIN32)
#define SWIFTMITM_LLHTTP_EXPORT __declspec(dllexport)
#else
#define SWIFTMITM_LLHTTP_EXPORT
#endif

typedef c_swiftmitm_llhttp__internal_t swiftmitm_llhttp_t;
typedef struct swiftmitm_llhttp_settings_s swiftmitm_llhttp_settings_t;

typedef int (*swiftmitm_llhttp_data_cb)(swiftmitm_llhttp_t*, const char *at, size_t length);
typedef int (*swiftmitm_llhttp_cb)(swiftmitm_llhttp_t*);

struct swiftmitm_llhttp_settings_s {
  /* Possible return values 0, -1, `HPE_PAUSED` */
  swiftmitm_llhttp_cb      on_message_begin;

  /* Possible return values 0, -1, HPE_USER */
  swiftmitm_llhttp_data_cb on_protocol;
  swiftmitm_llhttp_data_cb on_url;
  swiftmitm_llhttp_data_cb on_status;
  swiftmitm_llhttp_data_cb on_method;
  swiftmitm_llhttp_data_cb on_version;
  swiftmitm_llhttp_data_cb on_header_field;
  swiftmitm_llhttp_data_cb on_header_value;
  swiftmitm_llhttp_data_cb      on_chunk_extension_name;
  swiftmitm_llhttp_data_cb      on_chunk_extension_value;

  /* Possible return values:
   * 0  - Proceed normally
   * 1  - Assume that request/response has no body, and proceed to parsing the
   *      next message
   * 2  - Assume absence of body (as above) and make `c_nio_llhttp_execute()` return
   *      `HPE_PAUSED_UPGRADE`
   * -1 - Error
   * `HPE_PAUSED`
   */
  swiftmitm_llhttp_cb      on_headers_complete;

  /* Possible return values 0, -1, HPE_USER */
  swiftmitm_llhttp_data_cb on_body;

  /* Possible return values 0, -1, `HPE_PAUSED` */
  swiftmitm_llhttp_cb      on_message_complete;
  swiftmitm_llhttp_cb      on_protocol_complete;
  swiftmitm_llhttp_cb      on_url_complete;
  swiftmitm_llhttp_cb      on_status_complete;
  swiftmitm_llhttp_cb      on_method_complete;
  swiftmitm_llhttp_cb      on_version_complete;
  swiftmitm_llhttp_cb      on_header_field_complete;
  swiftmitm_llhttp_cb      on_header_value_complete;
  swiftmitm_llhttp_cb      on_chunk_extension_name_complete;
  swiftmitm_llhttp_cb      on_chunk_extension_value_complete;

  /* When on_chunk_header is called, the current chunk length is stored
   * in parser->content_length.
   * Possible return values 0, -1, `HPE_PAUSED`
   */
  swiftmitm_llhttp_cb      on_chunk_header;
  swiftmitm_llhttp_cb      on_chunk_complete;
  swiftmitm_llhttp_cb      on_reset;
};

/* Initialize the parser with specific type and user settings.
 *
 * NOTE: lifetime of `settings` has to be at least the same as the lifetime of
 * the `parser` here. In practice, `settings` has to be either a static
 * variable or be allocated with `malloc`, `new`, etc.
 */
SWIFTMITM_LLHTTP_EXPORT
void c_swiftmitm_llhttp_init(swiftmitm_llhttp_t* parser, swiftmitm_llhttp_type_t type,
                 const swiftmitm_llhttp_settings_t* settings);

SWIFTMITM_LLHTTP_EXPORT
swiftmitm_llhttp_t* swiftmitm_llhttp_alloc(swiftmitm_llhttp_type_t type);

SWIFTMITM_LLHTTP_EXPORT
void swiftmitm_llhttp_free(swiftmitm_llhttp_t* parser);

SWIFTMITM_LLHTTP_EXPORT
uint8_t c_swiftmitm_llhttp_get_type(swiftmitm_llhttp_t* parser);

SWIFTMITM_LLHTTP_EXPORT
uint8_t c_swiftmitm_llhttp_get_http_major(swiftmitm_llhttp_t* parser);

SWIFTMITM_LLHTTP_EXPORT
uint8_t c_swiftmitm_llhttp_get_http_minor(swiftmitm_llhttp_t* parser);

SWIFTMITM_LLHTTP_EXPORT
uint8_t c_swiftmitm_llhttp_get_method(swiftmitm_llhttp_t* parser);

SWIFTMITM_LLHTTP_EXPORT
int c_swiftmitm_llhttp_get_status_code(swiftmitm_llhttp_t* parser);

SWIFTMITM_LLHTTP_EXPORT
uint8_t c_swiftmitm_llhttp_get_upgrade(swiftmitm_llhttp_t* parser);

/* Reset an already initialized parser back to the start state, preserving the
 * existing parser type, callback settings, user data, and lenient flags.
 */
SWIFTMITM_LLHTTP_EXPORT
void c_swiftmitm_llhttp_reset(swiftmitm_llhttp_t* parser);

/* Initialize the settings object */
SWIFTMITM_LLHTTP_EXPORT
void c_swiftmitm_llhttp_settings_init(swiftmitm_llhttp_settings_t* settings);

/* Parse full or partial request/response, invoking user callbacks along the
 * way.
 *
 * If any of `llhttp_data_cb` returns errno not equal to `HPE_OK` - the parsing
 * interrupts, and such errno is returned from `c_nio_llhttp_execute()`. If
 * `HPE_PAUSED` was used as a errno, the execution can be resumed with
 * `c_nio_llhttp_resume()` call.
 *
 * In a special case of CONNECT/Upgrade request/response `HPE_PAUSED_UPGRADE`
 * is returned after fully parsing the request/response. If the user wishes to
 * continue parsing, they need to invoke `c_nio_llhttp_resume_after_upgrade()`.
 *
 * NOTE: if this function ever returns a non-pause type error, it will continue
 * to return the same error upon each successive call up until `c_nio_llhttp_init()`
 * is called.
 */
SWIFTMITM_LLHTTP_EXPORT
swiftmitm_llhttp_errno_t c_swiftmitm_llhttp_execute(swiftmitm_llhttp_t* parser, const char* data, size_t len);

/* This method should be called when the other side has no further bytes to
 * send (e.g. shutdown of readable side of the TCP connection.)
 *
 * Requests without `Content-Length` and other messages might require treating
 * all incoming bytes as the part of the body, up to the last byte of the
 * connection. This method will invoke `on_message_complete()` callback if the
 * request was terminated safely. Otherwise a error code would be returned.
 */
SWIFTMITM_LLHTTP_EXPORT
swiftmitm_llhttp_errno_t c_swiftmitm_llhttp_finish(swiftmitm_llhttp_t* parser);

/* Returns `1` if the incoming message is parsed until the last byte, and has
 * to be completed by calling `c_nio_llhttp_finish()` on EOF
 */
SWIFTMITM_LLHTTP_EXPORT
int c_swiftmitm_llhttp_message_needs_eof(const swiftmitm_llhttp_t* parser);

/* Returns `1` if there might be any other messages following the last that was
 * successfully parsed.
 */
SWIFTMITM_LLHTTP_EXPORT
int c_swiftmitm_llhttp_should_keep_alive(const swiftmitm_llhttp_t* parser);

/* Make further calls of `c_nio_llhttp_execute()` return `HPE_PAUSED` and set
 * appropriate error reason.
 *
 * Important: do not call this from user callbacks! User callbacks must return
 * `HPE_PAUSED` if pausing is required.
 */
SWIFTMITM_LLHTTP_EXPORT
void c_swiftmitm_llhttp_pause(swiftmitm_llhttp_t* parser);

/* Might be called to resume the execution after the pause in user's callback.
 * See `c_nio_llhttp_execute()` above for details.
 *
 * Call this only if `c_nio_llhttp_execute()` returns `HPE_PAUSED`.
 */
SWIFTMITM_LLHTTP_EXPORT
void c_swiftmitm_llhttp_resume(swiftmitm_llhttp_t* parser);

/* Might be called to resume the execution after the pause in user's callback.
 * See `c_nio_llhttp_execute()` above for details.
 *
 * Call this only if `c_nio_llhttp_execute()` returns `HPE_PAUSED_UPGRADE`
 */
SWIFTMITM_LLHTTP_EXPORT
void c_swiftmitm_llhttp_resume_after_upgrade(swiftmitm_llhttp_t* parser);

/* Returns the latest return error */
SWIFTMITM_LLHTTP_EXPORT
swiftmitm_llhttp_errno_t c_swiftmitm_llhttp_get_errno(const swiftmitm_llhttp_t* parser);

/* Returns the verbal explanation of the latest returned error.
 *
 * Note: User callback should set error reason when returning the error. See
 * `c_nio_llhttp_set_error_reason()` for details.
 */
SWIFTMITM_LLHTTP_EXPORT
const char* c_swiftmitm_llhttp_get_error_reason(const swiftmitm_llhttp_t* parser);

/* Assign verbal description to the returned error. Must be called in user
 * callbacks right before returning the errno.
 *
 * Note: `HPE_USER` error code might be useful in user callbacks.
 */
SWIFTMITM_LLHTTP_EXPORT
void c_swiftmitm_llhttp_set_error_reason(swiftmitm_llhttp_t* parser, const char* reason);

/* Returns the pointer to the last parsed byte before the returned error. The
 * pointer is relative to the `data` argument of `c_nio_llhttp_execute()`.
 *
 * Note: this method might be useful for counting the number of parsed bytes.
 */
SWIFTMITM_LLHTTP_EXPORT
const char* c_swiftmitm_llhttp_get_error_pos(const swiftmitm_llhttp_t* parser);

/* Returns textual name of error code */
SWIFTMITM_LLHTTP_EXPORT
const char* c_swiftmitm_llhttp_errno_name(swiftmitm_llhttp_errno_t err);

/* Returns textual name of HTTP method */
SWIFTMITM_LLHTTP_EXPORT
const char* c_swiftmitm_llhttp_method_name(swiftmitm_llhttp_method_t method);

/* Returns textual name of HTTP status */
SWIFTMITM_LLHTTP_EXPORT
const char* c_swiftmitm_llhttp_status_name(swiftmitm_llhttp_status_t status);

/* Enables/disables lenient header value parsing (disabled by default).
 *
 * Lenient parsing disables header value token checks, extending llhttp's
 * protocol support to highly non-compliant clients/server. No
 * `HPE_INVALID_HEADER_TOKEN` will be raised for incorrect header values when
 * lenient parsing is "on".
 *
 * **Enabling this flag can pose a security issue since you will be exposed to
 * request smuggling attacks. USE WITH CAUTION!**
 */
SWIFTMITM_LLHTTP_EXPORT
void c_swiftmitm_llhttp_set_lenient_headers(swiftmitm_llhttp_t* parser, int enabled);


/* Enables/disables lenient handling of conflicting `Transfer-Encoding` and
 * `Content-Length` headers (disabled by default).
 *
 * Normally `llhttp` would error when `Transfer-Encoding` is present in
 * conjunction with `Content-Length`. This error is important to prevent HTTP
 * request smuggling, but may be less desirable for small number of cases
 * involving legacy servers.
 *
 * **Enabling this flag can pose a security issue since you will be exposed to
 * request smuggling attacks. USE WITH CAUTION!**
 */
SWIFTMITM_LLHTTP_EXPORT
void c_swiftmitm_llhttp_set_lenient_chunked_length(swiftmitm_llhttp_t* parser, int enabled);


/* Enables/disables lenient handling of `Connection: close` and HTTP/1.0
 * requests responses.
 *
 * Normally `llhttp` would error on (in strict mode) or discard (in loose mode)
 * the HTTP request/response after the request/response with `Connection: close`
 * and `Content-Length`. This is important to prevent cache poisoning attacks,
 * but might interact badly with outdated and insecure clients. With this flag
 * the extra request/response will be parsed normally.
 *
 * **Enabling this flag can pose a security issue since you will be exposed to
 * poisoning attacks. USE WITH CAUTION!**
 */
SWIFTMITM_LLHTTP_EXPORT
void c_swiftmitm_llhttp_set_lenient_keep_alive(swiftmitm_llhttp_t* parser, int enabled);

/* Enables/disables lenient handling of `Transfer-Encoding` header.
 *
 * Normally `llhttp` would error when a `Transfer-Encoding` has `chunked` value
 * and another value after it (either in a single header or in multiple
 * headers whose value are internally joined using `, `).
 * This is mandated by the spec to reliably determine request body size and thus
 * avoid request smuggling.
 * With this flag the extra value will be parsed normally.
 *
 * **Enabling this flag can pose a security issue since you will be exposed to
 * request smuggling attacks. USE WITH CAUTION!**
 */
SWIFTMITM_LLHTTP_EXPORT
void c_swiftmitm_llhttp_set_lenient_transfer_encoding(swiftmitm_llhttp_t* parser, int enabled);

/* Enables/disables lenient handling of HTTP version.
 *
 * Normally `llhttp` would error when the HTTP version in the request or status line
 * is not `0.9`, `1.0`, `1.1` or `2.0`.
 * With this flag the invalid value will be parsed normally.
 *
 * **Enabling this flag can pose a security issue since you will allow unsupported
 * HTTP versions. USE WITH CAUTION!**
 */
SWIFTMITM_LLHTTP_EXPORT
void c_swiftmitm_llhttp_set_lenient_version(swiftmitm_llhttp_t* parser, int enabled);

/* Enables/disables lenient handling of additional data received after a message ends
 * and keep-alive is disabled.
 *
 * Normally `llhttp` would error when additional unexpected data is received if the message
 * contains the `Connection` header with `close` value.
 * With this flag the extra data will discarded without throwing an error.
 *
 * **Enabling this flag can pose a security issue since you will be exposed to
 * poisoning attacks. USE WITH CAUTION!**
 */
SWIFTMITM_LLHTTP_EXPORT
void c_swiftmitm_llhttp_set_lenient_data_after_close(swiftmitm_llhttp_t* parser, int enabled);

/* Enables/disables lenient handling of incomplete CRLF sequences.
 *
 * Normally `llhttp` would error when a CR is not followed by LF when terminating the
 * request line, the status line, the headers or a chunk header.
 * With this flag only a CR is required to terminate such sections.
 *
 * **Enabling this flag can pose a security issue since you will be exposed to
 * request smuggling attacks. USE WITH CAUTION!**
 */
SWIFTMITM_LLHTTP_EXPORT
void c_swiftmitm_llhttp_set_lenient_optional_lf_after_cr(swiftmitm_llhttp_t* parser, int enabled);

/*
 * Enables/disables lenient handling of line separators.
 *
 * Normally `llhttp` would error when a LF is not preceded by CR when terminating the
 * request line, the status line, the headers, a chunk header or a chunk data.
 * With this flag only a LF is required to terminate such sections.
 *
 * **Enabling this flag can pose a security issue since you will be exposed to
 * request smuggling attacks. USE WITH CAUTION!**
 */
SWIFTMITM_LLHTTP_EXPORT
void c_swiftmitm_llhttp_set_lenient_optional_cr_before_lf(swiftmitm_llhttp_t* parser, int enabled);

/* Enables/disables lenient handling of chunks not separated via CRLF.
 *
 * Normally `llhttp` would error when after a chunk data a CRLF is missing before
 * starting a new chunk.
 * With this flag the new chunk can start immediately after the previous one.
 *
 * **Enabling this flag can pose a security issue since you will be exposed to
 * request smuggling attacks. USE WITH CAUTION!**
 */
SWIFTMITM_LLHTTP_EXPORT
void c_swiftmitm_llhttp_set_lenient_optional_crlf_after_chunk(swiftmitm_llhttp_t* parser, int enabled);

/* Enables/disables lenient handling of spaces after chunk size.
 *
 * Normally `llhttp` would error when after a chunk size is followed by one or more
 * spaces are present instead of a CRLF or `;`.
 * With this flag this check is disabled.
 *
 * **Enabling this flag can pose a security issue since you will be exposed to
 * request smuggling attacks. USE WITH CAUTION!**
 */
SWIFTMITM_LLHTTP_EXPORT
void c_swiftmitm_llhttp_set_lenient_spaces_after_chunk_size(swiftmitm_llhttp_t* parser, int enabled);

#ifdef __cplusplus
}  /* extern "C" */
#endif
#endif  /* INCLUDE_LLHTTP_API_H_ */


#endif  /* INCLUDE_LLHTTP_H_ */
