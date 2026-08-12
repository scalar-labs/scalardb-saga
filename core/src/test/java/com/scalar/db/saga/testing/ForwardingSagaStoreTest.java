package com.scalar.db.saga.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.scalar.db.saga.api.SagaQuery;
import com.scalar.db.saga.api.SagaStateSnapshot;
import com.scalar.db.saga.api.SagaStatus;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.store.SagaStore;
import com.scalar.db.saga.store.SagaStore.ScanCursor;
import com.scalar.db.saga.store.StatusEvent;
import com.scalar.db.saga.store.StepEvent;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ForwardingSagaStoreTest {

  /** A forwarding store with no overrides — the base class's forwarders, exactly as shipped. */
  private static final class PlainForwardingStore extends ForwardingSagaStore {
    PlainForwardingStore(SagaStore delegate) {
      super(delegate);
    }
  }

  /**
   * Every hand-written forwarder must reach the delegate as the same interface method with the same
   * arguments in the same order. Enumerated by reflection so a forwarder added for a future {@code
   * SagaStore} method is covered automatically, and argument values are pairwise distinct so a
   * swapped-argument typo (which compiles fine for same-typed parameters) fails here.
   */
  @Test
  void allDeclaredForwarders_forwardToDelegateWithIdenticalArguments() throws Exception {
    // Arrange
    SagaStore delegate = mock(SagaStore.class);
    PlainForwardingStore forwarding = new PlainForwardingStore(delegate);

    List<String> checked = new ArrayList<>();
    for (Method forwarder : ForwardingSagaStore.class.getDeclaredMethods()) {
      if (!Modifier.isPublic(forwarder.getModifiers()) || forwarder.isSynthetic()) {
        continue; // skip the protected delegate() accessor and compiler-generated bridges
      }
      Method interfaceMethod =
          SagaStore.class.getMethod(forwarder.getName(), forwarder.getParameterTypes());
      Object[] args = distinctArgumentsFor(interfaceMethod);

      // Act
      interfaceMethod.invoke(forwarding, args);

      // Assert — the delegate received exactly that method with exactly those arguments
      try {
        interfaceMethod.invoke(verify(delegate), args);
        verifyNoMoreInteractions(delegate);
      } catch (InvocationTargetException e) {
        throw new AssertionError(
            "Forwarder did not forward faithfully: " + interfaceMethod, e.getCause());
      }
      clearInvocations(delegate);
      checked.add(interfaceMethod.getName());
    }

    // Sanity: the sweep saw the real forwarder set, not an accidentally empty loop
    assertThat(checked).hasSizeGreaterThanOrEqualTo(20);
  }

  @Test
  @SuppressWarnings("NullAway") // deliberately passing null to assert the constructor guard
  void constructor_nullDelegateGiven_throwsNullPointerException() {
    // Act & Assert
    assertThatThrownBy(() -> new PlainForwardingStore(null))
        .isInstanceOf(NullPointerException.class);
  }

  /**
   * Distinct dummy arguments for one method: same-typed parameters get different values, so the
   * exact-argument verification catches position swaps inside a forwarder.
   */
  private static Object[] distinctArgumentsFor(Method method) {
    Class<?>[] types = method.getParameterTypes();
    Object[] args = new Object[types.length];
    for (int i = 0; i < types.length; i++) {
      args[i] = dummyValue(types[i], i);
    }
    return args;
  }

  private static Object dummyValue(Class<?> type, int position) {
    if (type == String.class) {
      return "arg-" + position;
    }
    if (type == int.class || type == Integer.class) {
      return 100 + position;
    }
    if (type == Instant.class) {
      return Instant.ofEpochSecond(1_000_000L + position);
    }
    if (type == Map.class) {
      return Map.of("key-" + position, "value-" + position);
    }
    if (type == SagaStatus.class) {
      return SagaStatus.RUNNING;
    }
    if (type == SagaStateSnapshot.class) {
      Instant t = Instant.ofEpochSecond(2_000_000L + position);
      return new SagaStateSnapshot(
          "saga-" + position, "saga-name", SagaStatus.RUNNING, "owner-" + position, "v1", t, t);
    }
    if (type == StepEvent.class) {
      return StepEvent.completed(position, "step-" + position, null);
    }
    if (type == StatusEvent.class) {
      return StatusEvent.escalated("reason-" + position);
    }
    if (type == SagaDefinition.class) {
      return SagaDefinition.newBuilder("def-" + position)
          .saga()
          .step("step", "com.example.Step")
          .add()
          .build();
    }
    if (type == SagaQuery.class) {
      return SagaQuery.newBuilder().build();
    }
    if (type == ScanCursor.class) {
      return new ScanCursor() {};
    }
    throw new AssertionError(
        "No dummy value for parameter type "
            + type.getName()
            + " — extend dummyValue() for the new SagaStore method parameter");
  }
}
