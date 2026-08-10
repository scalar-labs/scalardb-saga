package com.scalar.db.saga.daemon.grpc;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.ErrorInfo;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;

/**
 * Test helper: extracts the {@link ErrorInfo} detail from a status, failing the test if the status
 * carries none. Works on a status built server-side and on one received over a transport, so the
 * mapper test and the interceptor tests assert the wire body the same way.
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
        try {
          return detail.unpack(ErrorInfo.class);
        } catch (InvalidProtocolBufferException malformed) {
          throw new AssertionError("malformed ErrorInfo detail", malformed);
        }
      }
    }
    throw new AssertionError("status carried no ErrorInfo detail");
  }
}
