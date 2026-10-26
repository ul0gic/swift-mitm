/* Additional changes for SwiftNIO:
    - prefixed all symbols by 'c_nio_'
*/
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "include/swiftmitm_llhttp.h"

#define CALLBACK_MAYBE(PARSER, NAME)                                          \
  do {                                                                        \
    const swiftmitm_llhttp_settings_t* settings;                                        \
    settings = (const swiftmitm_llhttp_settings_t*) (PARSER)->settings;                 \
    if (settings == NULL || settings->NAME == NULL) {                         \
      err = 0;                                                                \
      break;                                                                  \
    }                                                                         \
    err = settings->NAME((PARSER));                                           \
  } while (0)

#define SPAN_CALLBACK_MAYBE(PARSER, NAME, START, LEN)                         \
  do {                                                                        \
    const swiftmitm_llhttp_settings_t* settings;                                        \
    settings = (const swiftmitm_llhttp_settings_t*) (PARSER)->settings;                 \
    if (settings == NULL || settings->NAME == NULL) {                         \
      err = 0;                                                                \
      break;                                                                  \
    }                                                                         \
    err = settings->NAME((PARSER), (START), (LEN));                           \
    if (err == -1) {                                                          \
      err = SWIFTMITM_HPE_USER;                                                         \
      c_swiftmitm_llhttp_set_error_reason((PARSER), "Span callback error in " #NAME);     \
    }                                                                         \
  } while (0)

void c_swiftmitm_llhttp_init(swiftmitm_llhttp_t* parser, swiftmitm_llhttp_type_t type,
                 const swiftmitm_llhttp_settings_t* settings) {
  c_swiftmitm_llhttp__internal_init(parser);

  parser->type = type;
  parser->settings = (void*) settings;
}


#if defined(__wasm__)

extern int swiftmitm_wasm_on_message_begin(swiftmitm_llhttp_t * p);
extern int swiftmitm_wasm_on_url(swiftmitm_llhttp_t* p, const char* at, size_t length);
extern int swiftmitm_wasm_on_status(swiftmitm_llhttp_t* p, const char* at, size_t length);
extern int swiftmitm_wasm_on_header_field(swiftmitm_llhttp_t* p, const char* at, size_t length);
extern int swiftmitm_wasm_on_header_value(swiftmitm_llhttp_t* p, const char* at, size_t length);
extern int swiftmitm_wasm_on_headers_complete(swiftmitm_llhttp_t * p, int status_code,
                                    uint8_t upgrade, int should_keep_alive);
extern int swiftmitm_wasm_on_body(swiftmitm_llhttp_t* p, const char* at, size_t length);
extern int swiftmitm_wasm_on_message_complete(swiftmitm_llhttp_t * p);

static int swiftmitm_wasm_on_headers_complete_wrap(swiftmitm_llhttp_t* p) {
  return swiftmitm_wasm_on_headers_complete(p, p->status_code, p->upgrade,
                                  c_swiftmitm_llhttp_should_keep_alive(p));
}

const swiftmitm_llhttp_settings_t wasm_settings = {
  .on_message_begin = swiftmitm_wasm_on_message_begin,
  .on_url = swiftmitm_wasm_on_url,
  .on_status = swiftmitm_wasm_on_status,
  .on_header_field = swiftmitm_wasm_on_header_field,
  .on_header_value = swiftmitm_wasm_on_header_value,
  .on_headers_complete = swiftmitm_wasm_on_headers_complete_wrap,
  .on_body = swiftmitm_wasm_on_body,
  .on_message_complete = swiftmitm_wasm_on_message_complete,
};


swiftmitm_llhttp_t* swiftmitm_llhttp_alloc(swiftmitm_llhttp_type_t type) {
  swiftmitm_llhttp_t* parser = malloc(sizeof(swiftmitm_llhttp_t));
  c_swiftmitm_llhttp_init(parser, type, &wasm_settings);
  return parser;
}

void swiftmitm_llhttp_free(swiftmitm_llhttp_t* parser) {
  free(parser);
}

#endif  // defined(__wasm__)

/* Some getters required to get stuff from the parser */

uint8_t c_swiftmitm_llhttp_get_type(swiftmitm_llhttp_t* parser) {
  return parser->type;
}

uint8_t c_swiftmitm_llhttp_get_http_major(swiftmitm_llhttp_t* parser) {
  return parser->http_major;
}

uint8_t c_swiftmitm_llhttp_get_http_minor(swiftmitm_llhttp_t* parser) {
  return parser->http_minor;
}

uint8_t c_swiftmitm_llhttp_get_method(swiftmitm_llhttp_t* parser) {
  return parser->method;
}

int c_swiftmitm_llhttp_get_status_code(swiftmitm_llhttp_t* parser) {
  return parser->status_code;
}

uint8_t c_swiftmitm_llhttp_get_upgrade(swiftmitm_llhttp_t* parser) {
  return parser->upgrade;
}


void c_swiftmitm_llhttp_reset(swiftmitm_llhttp_t* parser) {
  swiftmitm_llhttp_type_t type = parser->type;
  const swiftmitm_llhttp_settings_t* settings = parser->settings;
  void* data = parser->data;
  uint16_t lenient_flags = parser->lenient_flags;

  c_swiftmitm_llhttp__internal_init(parser);

  parser->type = type;
  parser->settings = (void*) settings;
  parser->data = data;
  parser->lenient_flags = lenient_flags;
}


swiftmitm_llhttp_errno_t c_swiftmitm_llhttp_execute(swiftmitm_llhttp_t* parser, const char* data, size_t len) {
  return c_swiftmitm_llhttp__internal_execute(parser, data, data + len);
}


void c_swiftmitm_llhttp_settings_init(swiftmitm_llhttp_settings_t* settings) {
  memset(settings, 0, sizeof(*settings));
}


swiftmitm_llhttp_errno_t c_swiftmitm_llhttp_finish(swiftmitm_llhttp_t* parser) {
  int err;

  /* We're in an error state. Don't bother doing anything. */
  if (parser->error != 0) {
    return 0;
  }

  switch (parser->finish) {
    case SWIFTMITM_HTTP_FINISH_SAFE_WITH_CB:
      CALLBACK_MAYBE(parser, on_message_complete);
      if (err != SWIFTMITM_HPE_OK) return err;

    /* FALLTHROUGH */
    case SWIFTMITM_HTTP_FINISH_SAFE:
      return SWIFTMITM_HPE_OK;
    case SWIFTMITM_HTTP_FINISH_UNSAFE:
      parser->reason = "Invalid EOF state";
      return SWIFTMITM_HPE_INVALID_EOF_STATE;
    default:
      abort();
  }
}


void c_swiftmitm_llhttp_pause(swiftmitm_llhttp_t* parser) {
  if (parser->error != SWIFTMITM_HPE_OK) {
    return;
  }

  parser->error = SWIFTMITM_HPE_PAUSED;
  parser->reason = "Paused";
}


void c_swiftmitm_llhttp_resume(swiftmitm_llhttp_t* parser) {
  if (parser->error != SWIFTMITM_HPE_PAUSED) {
    return;
  }

  parser->error = 0;
}


void c_swiftmitm_llhttp_resume_after_upgrade(swiftmitm_llhttp_t* parser) {
  if (parser->error != SWIFTMITM_HPE_PAUSED_UPGRADE) {
    return;
  }

  parser->error = 0;
}


swiftmitm_llhttp_errno_t c_swiftmitm_llhttp_get_errno(const swiftmitm_llhttp_t* parser) {
  return parser->error;
}


const char* c_swiftmitm_llhttp_get_error_reason(const swiftmitm_llhttp_t* parser) {
  return parser->reason;
}


void c_swiftmitm_llhttp_set_error_reason(swiftmitm_llhttp_t* parser, const char* reason) {
  parser->reason = reason;
}


const char* c_swiftmitm_llhttp_get_error_pos(const swiftmitm_llhttp_t* parser) {
  return parser->error_pos;
}


const char* c_swiftmitm_llhttp_errno_name(swiftmitm_llhttp_errno_t err) {
#define SWIFTMITM_HTTP_ERRNO_GEN(CODE, NAME, _) case SWIFTMITM_HPE_##NAME: return "HPE_" #NAME;
  switch (err) {
    SWIFTMITM_HTTP_ERRNO_MAP(SWIFTMITM_HTTP_ERRNO_GEN)
    default: abort();
  }
#undef SWIFTMITM_HTTP_ERRNO_GEN
}


const char* c_swiftmitm_llhttp_method_name(swiftmitm_llhttp_method_t method) {
#define SWIFTMITM_HTTP_METHOD_GEN(NUM, NAME, STRING) case SWIFTMITM_HTTP_##NAME: return #STRING;
  switch (method) {
    SWIFTMITM_HTTP_ALL_METHOD_MAP(SWIFTMITM_HTTP_METHOD_GEN)
    default: abort();
  }
#undef SWIFTMITM_HTTP_METHOD_GEN
}

const char* c_swiftmitm_llhttp_status_name(swiftmitm_llhttp_status_t status) {
#define SWIFTMITM_HTTP_STATUS_GEN(NUM, NAME, STRING) case SWIFTMITM_HTTP_STATUS_##NAME: return #STRING;
  switch (status) {
    SWIFTMITM_HTTP_STATUS_MAP(SWIFTMITM_HTTP_STATUS_GEN)
    default: abort();
  }
#undef SWIFTMITM_HTTP_STATUS_GEN
}


void c_swiftmitm_llhttp_set_lenient_headers(swiftmitm_llhttp_t* parser, int enabled) {
  if (enabled) {
    parser->lenient_flags |= SWIFTMITM_LENIENT_HEADERS;
  } else {
    parser->lenient_flags &= ~SWIFTMITM_LENIENT_HEADERS;
  }
}


void c_swiftmitm_llhttp_set_lenient_chunked_length(swiftmitm_llhttp_t* parser, int enabled) {
  if (enabled) {
    parser->lenient_flags |= SWIFTMITM_LENIENT_CHUNKED_LENGTH;
  } else {
    parser->lenient_flags &= ~SWIFTMITM_LENIENT_CHUNKED_LENGTH;
  }
}


void c_swiftmitm_llhttp_set_lenient_keep_alive(swiftmitm_llhttp_t* parser, int enabled) {
  if (enabled) {
    parser->lenient_flags |= SWIFTMITM_LENIENT_KEEP_ALIVE;
  } else {
    parser->lenient_flags &= ~SWIFTMITM_LENIENT_KEEP_ALIVE;
  }
}

void c_swiftmitm_llhttp_set_lenient_transfer_encoding(swiftmitm_llhttp_t* parser, int enabled) {
  if (enabled) {
    parser->lenient_flags |= SWIFTMITM_LENIENT_TRANSFER_ENCODING;
  } else {
    parser->lenient_flags &= ~SWIFTMITM_LENIENT_TRANSFER_ENCODING;
  }
}

void c_swiftmitm_llhttp_set_lenient_version(swiftmitm_llhttp_t* parser, int enabled) {
  if (enabled) {
    parser->lenient_flags |= SWIFTMITM_LENIENT_VERSION;
  } else {
    parser->lenient_flags &= ~SWIFTMITM_LENIENT_VERSION;
  }
}

void c_swiftmitm_llhttp_set_lenient_data_after_close(swiftmitm_llhttp_t* parser, int enabled) {
  if (enabled) {
    parser->lenient_flags |= SWIFTMITM_LENIENT_DATA_AFTER_CLOSE;
  } else {
    parser->lenient_flags &= ~SWIFTMITM_LENIENT_DATA_AFTER_CLOSE;
  }
}

void c_swiftmitm_llhttp_set_lenient_optional_lf_after_cr(swiftmitm_llhttp_t* parser, int enabled) {
  if (enabled) {
    parser->lenient_flags |= SWIFTMITM_LENIENT_OPTIONAL_LF_AFTER_CR;
  } else {
    parser->lenient_flags &= ~SWIFTMITM_LENIENT_OPTIONAL_LF_AFTER_CR;
  }
}

void c_swiftmitm_llhttp_set_lenient_optional_crlf_after_chunk(swiftmitm_llhttp_t* parser, int enabled) {
  if (enabled) {
    parser->lenient_flags |= SWIFTMITM_LENIENT_OPTIONAL_CRLF_AFTER_CHUNK;
  } else {
    parser->lenient_flags &= ~SWIFTMITM_LENIENT_OPTIONAL_CRLF_AFTER_CHUNK;
  }
}

void c_swiftmitm_llhttp_set_lenient_optional_cr_before_lf(swiftmitm_llhttp_t* parser, int enabled) {
  if (enabled) {
    parser->lenient_flags |= SWIFTMITM_LENIENT_OPTIONAL_CR_BEFORE_LF;
  } else {
    parser->lenient_flags &= ~SWIFTMITM_LENIENT_OPTIONAL_CR_BEFORE_LF;
  }
}

void c_swiftmitm_llhttp_set_lenient_spaces_after_chunk_size(swiftmitm_llhttp_t* parser, int enabled) {
  if (enabled) {
    parser->lenient_flags |= SWIFTMITM_LENIENT_SPACES_AFTER_CHUNK_SIZE;
  } else {
    parser->lenient_flags &= ~SWIFTMITM_LENIENT_SPACES_AFTER_CHUNK_SIZE;
  }
}

/* Callbacks */


int c_swiftmitm_llhttp__on_message_begin(swiftmitm_llhttp_t* s, const char* p, const char* endp) {
  int err;
  CALLBACK_MAYBE(s, on_message_begin);
  return err;
}


int c_swiftmitm_llhttp__on_protocol(swiftmitm_llhttp_t* s, const char* p, const char* endp) {
  int err;
  SPAN_CALLBACK_MAYBE(s, on_protocol, p, endp - p);
  return err;
}


int c_swiftmitm_llhttp__on_protocol_complete(swiftmitm_llhttp_t* s, const char* p, const char* endp) {
  int err;
  CALLBACK_MAYBE(s, on_protocol_complete);
  return err;
}


int c_swiftmitm_llhttp__on_url(swiftmitm_llhttp_t* s, const char* p, const char* endp) {
  int err;
  SPAN_CALLBACK_MAYBE(s, on_url, p, endp - p);
  return err;
}


int c_swiftmitm_llhttp__on_url_complete(swiftmitm_llhttp_t* s, const char* p, const char* endp) {
  int err;
  CALLBACK_MAYBE(s, on_url_complete);
  return err;
}


int c_swiftmitm_llhttp__on_status(swiftmitm_llhttp_t* s, const char* p, const char* endp) {
  int err;
  SPAN_CALLBACK_MAYBE(s, on_status, p, endp - p);
  return err;
}


int c_swiftmitm_llhttp__on_status_complete(swiftmitm_llhttp_t* s, const char* p, const char* endp) {
  int err;
  CALLBACK_MAYBE(s, on_status_complete);
  return err;
}


int c_swiftmitm_llhttp__on_method(swiftmitm_llhttp_t* s, const char* p, const char* endp) {
  int err;
  SPAN_CALLBACK_MAYBE(s, on_method, p, endp - p);
  return err;
}


int c_swiftmitm_llhttp__on_method_complete(swiftmitm_llhttp_t* s, const char* p, const char* endp) {
  int err;
  CALLBACK_MAYBE(s, on_method_complete);
  return err;
}


int c_swiftmitm_llhttp__on_version(swiftmitm_llhttp_t* s, const char* p, const char* endp) {
  int err;
  SPAN_CALLBACK_MAYBE(s, on_version, p, endp - p);
  return err;
}


int c_swiftmitm_llhttp__on_version_complete(swiftmitm_llhttp_t* s, const char* p, const char* endp) {
  int err;
  CALLBACK_MAYBE(s, on_version_complete);
  return err;
}


int c_swiftmitm_llhttp__on_header_field(swiftmitm_llhttp_t* s, const char* p, const char* endp) {
  int err;
  SPAN_CALLBACK_MAYBE(s, on_header_field, p, endp - p);
  return err;
}


int c_swiftmitm_llhttp__on_header_field_complete(swiftmitm_llhttp_t* s, const char* p, const char* endp) {
  int err;
  CALLBACK_MAYBE(s, on_header_field_complete);
  return err;
}


int c_swiftmitm_llhttp__on_header_value(swiftmitm_llhttp_t* s, const char* p, const char* endp) {
  int err;
  SPAN_CALLBACK_MAYBE(s, on_header_value, p, endp - p);
  return err;
}


int c_swiftmitm_llhttp__on_header_value_complete(swiftmitm_llhttp_t* s, const char* p, const char* endp) {
  int err;
  CALLBACK_MAYBE(s, on_header_value_complete);
  return err;
}


int c_swiftmitm_llhttp__on_headers_complete(swiftmitm_llhttp_t* s, const char* p, const char* endp) {
  int err;
  CALLBACK_MAYBE(s, on_headers_complete);
  return err;
}


int c_swiftmitm_llhttp__on_message_complete(swiftmitm_llhttp_t* s, const char* p, const char* endp) {
  int err;
  CALLBACK_MAYBE(s, on_message_complete);
  return err;
}


int c_swiftmitm_llhttp__on_body(swiftmitm_llhttp_t* s, const char* p, const char* endp) {
  int err;
  SPAN_CALLBACK_MAYBE(s, on_body, p, endp - p);
  return err;
}


int c_swiftmitm_llhttp__on_chunk_header(swiftmitm_llhttp_t* s, const char* p, const char* endp) {
  int err;
  CALLBACK_MAYBE(s, on_chunk_header);
  return err;
}


int c_swiftmitm_llhttp__on_chunk_extension_name(swiftmitm_llhttp_t* s, const char* p, const char* endp) {
  int err;
  SPAN_CALLBACK_MAYBE(s, on_chunk_extension_name, p, endp - p);
  return err;
}


int c_swiftmitm_llhttp__on_chunk_extension_name_complete(swiftmitm_llhttp_t* s, const char* p, const char* endp) {
  int err;
  CALLBACK_MAYBE(s, on_chunk_extension_name_complete);
  return err;
}


int c_swiftmitm_llhttp__on_chunk_extension_value(swiftmitm_llhttp_t* s, const char* p, const char* endp) {
  int err;
  SPAN_CALLBACK_MAYBE(s, on_chunk_extension_value, p, endp - p);
  return err;
}


int c_swiftmitm_llhttp__on_chunk_extension_value_complete(swiftmitm_llhttp_t* s, const char* p, const char* endp) {
  int err;
  CALLBACK_MAYBE(s, on_chunk_extension_value_complete);
  return err;
}


int c_swiftmitm_llhttp__on_chunk_complete(swiftmitm_llhttp_t* s, const char* p, const char* endp) {
  int err;
  CALLBACK_MAYBE(s, on_chunk_complete);
  return err;
}


int c_swiftmitm_llhttp__on_reset(swiftmitm_llhttp_t* s, const char* p, const char* endp) {
  int err;
  CALLBACK_MAYBE(s, on_reset);
  return err;
}


/* Private */


void c_swiftmitm_llhttp__debug(swiftmitm_llhttp_t* s, const char* p, const char* endp,
                   const char* msg) {
  if (p == endp) {
    fprintf(stderr, "p=%p type=%d flags=%02x next=null debug=%s\n", s, s->type,
            s->flags, msg);
  } else {
    fprintf(stderr, "p=%p type=%d flags=%02x next=%02x   debug=%s\n", s,
            s->type, s->flags, *p, msg);
  }
}
