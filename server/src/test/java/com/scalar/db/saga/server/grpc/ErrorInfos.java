package com.scalar.db.saga.server.grpc;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.ErrorInfo;
import com.scalar.db.saga.exception.SagaErrorCode;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;

/**
 * Test helper: extracts the {@link ErrorInfo} detail from a status, failing the test if the status
 * carries none — or if the detail's domain is not {@link SagaErrorCode#WIRE_DOMAIN}, so every test
 * that reads an {@code ErrorInfo} also pins the domain the client SDK filters on. Works on a status
 * built server-side and on one received over a transport, so the mapper test and the interceptor
 * tests assert the wire body the same way.
 */
final class ErrorInfos {

  private ErrorInfos() {}

  static ErrorInfo errorInfo(StatusRuntimeException e) {
    com.google.rpc.Status status = StatusProto.fromThrowable(e);
    if (status == null) {
      throw new AssertionError("status carried no google.rpc.Status details");
    }
    for (Any detail : status.getDetailsList()) {
      if (detail.is(ErrorInfo.class)) {
        ErrorInfo info;
        try {
          info = detail.unpack(ErrorInfo.class);
        } catch (InvalidProtocolBufferException malformed) {
          throw new AssertionError("malformed ErrorInfo detail", malformed);
        }
        if (!SagaErrorCode.WIRE_DOMAIN.equals(info.getDomain())) {
          throw new AssertionError(
              "ErrorInfo domain must be "
                  + SagaErrorCode.WIRE_DOMAIN
                  + " (the client SDK filters on it) but was '"
                  + info.getDomain()
                  + "'");
        }
        return info;
      }
    }
    throw new AssertionError("status carried no ErrorInfo detail");
  }
}
