/* Additional changes for SwiftNIO:
    - prefixed all symbols by 'c_nio_'
*/
#include <stdio.h>
#ifndef SWIFTMITM_LLHTTP__TEST
# include "include/swiftmitm_llhttp.h"
#else
# define swiftmitm_llhttp_t llparse_t
#endif  /* */

int c_swiftmitm_llhttp_message_needs_eof(const swiftmitm_llhttp_t* parser);
int c_swiftmitm_llhttp_should_keep_alive(const swiftmitm_llhttp_t* parser);

int c_swiftmitm_llhttp__before_headers_complete(swiftmitm_llhttp_t* parser, const char* p,
                                    const char* endp) {
  /* Set this here so that on_headers_complete() callbacks can see it */
  if ((parser->flags & SWIFTMITM_F_UPGRADE) &&
      (parser->flags & SWIFTMITM_F_CONNECTION_UPGRADE)) {
    /* For responses, "Upgrade: foo" and "Connection: upgrade" are
     * mandatory only when it is a 101 Switching Protocols response,
     * otherwise it is purely informational, to announce support.
     */
    parser->upgrade =
        (parser->type == SWIFTMITM_HTTP_REQUEST || parser->status_code == 101);
  } else {
    parser->upgrade = (parser->method == SWIFTMITM_HTTP_CONNECT);
  }
  return 0;
}


/* Return values:
 * 0 - No body, `restart`, message_complete
 * 1 - CONNECT request, `restart`, message_complete, and pause
 * 2 - chunk_size_start
 * 3 - body_identity
 * 4 - body_identity_eof
 * 5 - invalid transfer-encoding for request
 */
int c_swiftmitm_llhttp__after_headers_complete(swiftmitm_llhttp_t* parser, const char* p,
                                   const char* endp) {
  int hasBody;

  hasBody = parser->flags & SWIFTMITM_F_CHUNKED || parser->content_length > 0;
  if (
      (parser->upgrade && (parser->method == SWIFTMITM_HTTP_CONNECT ||
                          (parser->flags & SWIFTMITM_F_SKIPBODY) || !hasBody)) ||
      /* See RFC 2616 section 4.4 - 1xx e.g. Continue */
      (parser->type == SWIFTMITM_HTTP_RESPONSE && parser->status_code == 101)
  ) {
    /* Exit, the rest of the message is in a different protocol. */
    return 1;
  }

  if (parser->type == SWIFTMITM_HTTP_RESPONSE && parser->status_code == 100) {
    /* No body, restart as the message is complete */
    return 0;
  }

  /* See RFC 2616 section 4.4 */
  if (
    parser->flags & SWIFTMITM_F_SKIPBODY ||         /* response to a HEAD request */
    (
      parser->type == SWIFTMITM_HTTP_RESPONSE && (
        parser->status_code == 102 ||     /* Processing */
        parser->status_code == 103 ||     /* Early Hints */
        parser->status_code == 204 ||     /* No Content */
        parser->status_code == 304        /* Not Modified */
      )
    )
  ) {
    return 0;
  } else if (parser->flags & SWIFTMITM_F_CHUNKED) {
    /* chunked encoding - ignore Content-Length header, prepare for a chunk */
    return 2;
  } else if (parser->flags & SWIFTMITM_F_TRANSFER_ENCODING) {
    if (parser->type == SWIFTMITM_HTTP_REQUEST &&
        (parser->lenient_flags & SWIFTMITM_LENIENT_CHUNKED_LENGTH) == 0 &&
        (parser->lenient_flags & SWIFTMITM_LENIENT_TRANSFER_ENCODING) == 0) {
      /* RFC 7230 3.3.3 */

      /* If a Transfer-Encoding header field
       * is present in a request and the chunked transfer coding is not
       * the final encoding, the message body length cannot be determined
       * reliably; the server MUST respond with the 400 (Bad Request)
       * status code and then close the connection.
       */
      return 5;
    } else {
      /* RFC 7230 3.3.3 */

      /* If a Transfer-Encoding header field is present in a response and
       * the chunked transfer coding is not the final encoding, the
       * message body length is determined by reading the connection until
       * it is closed by the server.
       */
      return 4;
    }
  } else {
    if (!(parser->flags & SWIFTMITM_F_CONTENT_LENGTH)) {
      if (!c_swiftmitm_llhttp_message_needs_eof(parser)) {
        /* Assume content-length 0 - read the next */
        return 0;
      } else {
        /* Read body until EOF */
        return 4;
      }
    } else if (parser->content_length == 0) {
      /* Content-Length header given but zero: Content-Length: 0\r\n */
      return 0;
    } else {
      /* Content-Length header given and non-zero */
      return 3;
    }
  }
}


int c_swiftmitm_llhttp__after_message_complete(swiftmitm_llhttp_t* parser, const char* p,
                                   const char* endp) {
  int should_keep_alive;

  should_keep_alive = c_swiftmitm_llhttp_should_keep_alive(parser);
  parser->finish = SWIFTMITM_HTTP_FINISH_SAFE;
  parser->flags = 0;

  /* NOTE: this is ignored in loose parsing mode */
  return should_keep_alive;
}


int c_swiftmitm_llhttp_message_needs_eof(const swiftmitm_llhttp_t* parser) {
  if (parser->type == SWIFTMITM_HTTP_REQUEST) {
    return 0;
  }

  /* See RFC 2616 section 4.4 */
  if (parser->status_code / 100 == 1 || /* 1xx e.g. Continue */
      parser->status_code == 204 ||     /* No Content */
      parser->status_code == 304 ||     /* Not Modified */
      (parser->flags & SWIFTMITM_F_SKIPBODY)) {     /* response to a HEAD request */
    return 0;
  }

  /* RFC 7230 3.3.3, see `c_nio_llhttp__after_headers_complete` */
  if ((parser->flags & SWIFTMITM_F_TRANSFER_ENCODING) &&
      (parser->flags & SWIFTMITM_F_CHUNKED) == 0) {
    return 1;
  }

  if (parser->flags & (SWIFTMITM_F_CHUNKED | SWIFTMITM_F_CONTENT_LENGTH)) {
    return 0;
  }

  return 1;
}


int c_swiftmitm_llhttp_should_keep_alive(const swiftmitm_llhttp_t* parser) {
  if (parser->http_major > 0 && parser->http_minor > 0) {
    /* HTTP/1.1 */
    if (parser->flags & SWIFTMITM_F_CONNECTION_CLOSE) {
      return 0;
    }
  } else {
    /* HTTP/1.0 or earlier */
    if (!(parser->flags & SWIFTMITM_F_CONNECTION_KEEP_ALIVE)) {
      return 0;
    }
  }

  return !c_swiftmitm_llhttp_message_needs_eof(parser);
}
