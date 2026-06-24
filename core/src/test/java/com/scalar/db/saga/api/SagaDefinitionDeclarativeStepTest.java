package com.scalar.db.saga.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaDefinition.SagaMode;
import com.scalar.db.saga.api.SagaDefinition.ServiceStep;
import com.scalar.db.saga.api.SagaDefinition.ServiceStep.Phase;
import com.scalar.db.saga.api.SagaDefinition.StepDefinition;
import com.scalar.db.saga.exception.SagaDefinitionException;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SagaDefinitionDeclarativeStepTest {

  private static HttpCall call(String path) {
    return HttpCall.newBuilder(path).build();
  }

  @Test
  void build_sagaPhasesGiven_buildsServiceStep() {
    // Arrange
    HttpCall execution =
        HttpCall.newBuilder("/debit")
            .jsonBody(Map.of("amount", "${amount}"))
            .output(Map.of("debitId", "$.debit_id"))
            .build();
    HttpCall compensation =
        HttpCall.newBuilder("/debit/reverse").jsonBody(Map.of("amount", "${amount}")).build();

    // Act
    SagaDefinition definition =
        SagaDefinition.newBuilder("transfer", SagaMode.SAGA)
            .serviceStep("debit", "account-service")
            .operation()
            .execution(execution)
            .compensation(compensation)
            .add()
            .build();

    // Assert
    StepDefinition step = definition.getSteps().get(0);
    assertThat(step).isInstanceOf(ServiceStep.class);
    ServiceStep service = (ServiceStep) step;
    assertThat(service.getService()).isEqualTo("account-service");
    assertThat(service.getTransport()).isEqualTo(CallSpec.Transport.HTTP);
    assertThat(service.isTcc()).isFalse();
    assertThat(service.getPhases().keySet())
        .containsExactlyInAnyOrder(Phase.EXECUTION, Phase.COMPENSATION);
    assertThat(service.getPhase(Phase.EXECUTION)).contains(execution);
    assertThat(service.getPhase(Phase.COMPENSATION)).contains(compensation);
    assertThat(service.getPhase(Phase.RESERVATION)).isEmpty();
  }

  @Test
  void build_tccPhasesGiven_buildsTccServiceStep() {
    // Act
    SagaDefinition definition =
        SagaDefinition.newBuilder("reserveSeats", SagaMode.TCC)
            .serviceStep("seat", "booking-service")
            .tccOperation()
            .reservation(call("/reserve"))
            .confirmation(call("/confirm"))
            .cancellation(call("/cancel"))
            .add()
            .build();

    // Assert
    ServiceStep step = (ServiceStep) definition.getSteps().get(0);
    assertThat(step.isTcc()).isTrue();
    assertThat(step.getPhases().keySet())
        .containsExactlyInAnyOrder(Phase.RESERVATION, Phase.CONFIRMATION, Phase.CANCELLATION);
  }

  @Test
  void build_sagaPhasesInTccMode_throwsException() {
    // Arrange
    SagaDefinition.Builder builder =
        SagaDefinition.newBuilder("tcc-saga", SagaMode.TCC)
            .serviceStep("debit", "account-service")
            .operation()
            .execution(call("/debit"))
            .compensation(call("/reverse"))
            .add();

    // Act & Assert
    assertThatThrownBy(builder::build).isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void build_tccPhasesInSagaMode_throwsException() {
    // Arrange
    SagaDefinition.Builder builder =
        SagaDefinition.newBuilder("saga", SagaMode.SAGA)
            .serviceStep("seat", "booking-service")
            .tccOperation()
            .reservation(call("/reserve"))
            .confirmation(call("/confirm"))
            .cancellation(call("/cancel"))
            .add();

    // Act & Assert
    assertThatThrownBy(builder::build).isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void add_incompleteSagaPhases_throwsException() {
    // Arrange — execution without compensation.
    SagaDefinition.DeclarativeStepBuilder stepBuilder =
        SagaDefinition.newBuilder("saga", SagaMode.SAGA)
            .serviceStep("debit", "account-service")
            .operation()
            .execution(call("/debit"));

    // Act & Assert — add() rejects the incomplete step.
    assertThatThrownBy(stepBuilder::add).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void add_incompleteTccPhases_throwsException() {
    // Arrange — reservation and confirmation without cancellation.
    SagaDefinition.TccDeclarativeStepBuilder stepBuilder =
        SagaDefinition.newBuilder("tcc", SagaMode.TCC)
            .serviceStep("seat", "booking-service")
            .tccOperation()
            .reservation(call("/reserve"))
            .confirmation(call("/confirm"));

    // Act & Assert — add() rejects the incomplete step.
    assertThatThrownBy(stepBuilder::add).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void execution_calledTwice_throwsException() {
    // Arrange
    SagaDefinition.DeclarativeStepBuilder stepBuilder =
        SagaDefinition.newBuilder("saga", SagaMode.SAGA)
            .serviceStep("debit", "account-service")
            .operation()
            .execution(call("/debit"));

    // Act & Assert — setting the same phase twice is rejected.
    assertThatThrownBy(() -> stepBuilder.execution(call("/debit-again")))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void reservation_calledTwice_throwsException() {
    // Arrange
    SagaDefinition.TccDeclarativeStepBuilder stepBuilder =
        SagaDefinition.newBuilder("tcc", SagaMode.TCC)
            .serviceStep("seat", "booking-service")
            .tccOperation()
            .reservation(call("/reserve"));

    // Act & Assert — setting the same phase twice is rejected.
    assertThatThrownBy(() -> stepBuilder.reservation(call("/reserve-again")))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void build_mixedStepKindsGiven_succeeds() {
    // Act
    SagaDefinition definition =
        SagaDefinition.newBuilder("mixed", SagaMode.SAGA)
            .step("classStep", "com.example.ClassStep")
            .add()
            .serviceStep("declStep", "account-service")
            .operation()
            .execution(call("/debit"))
            .compensation(call("/reverse"))
            .add()
            .build();

    // Assert
    assertThat(definition.getSteps()).hasSize(2);
    StepDefinition declStep = definition.getSteps().get(1);
    assertThat(declStep).isInstanceOf(ServiceStep.class);
    assertThat(((ServiceStep) declStep).getPhases().keySet())
        .containsExactlyInAnyOrder(Phase.EXECUTION, Phase.COMPENSATION);
  }

  @Test
  void serviceStep_blankNameGiven_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> SagaDefinition.newBuilder("s", SagaMode.SAGA).serviceStep(" ", "svc"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void serviceStep_blankServiceGiven_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> SagaDefinition.newBuilder("s", SagaMode.SAGA).serviceStep("step", " "))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void execution_nullCallGiven_throwsException() {
    // Act & Assert
    assertThatThrownBy(
            () ->
                SagaDefinition.newBuilder("s", SagaMode.SAGA)
                    .serviceStep("step", "svc")
                    .operation()
                    .execution(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void equals_equalServiceSteps_areEqual() {
    // Arrange
    ServiceStep a = buildSagaStep();
    ServiceStep b = buildSagaStep();

    // Act & Assert
    assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
  }

  private static ServiceStep buildSagaStep() {
    return (ServiceStep)
        SagaDefinition.newBuilder("s", SagaMode.SAGA)
            .serviceStep("step", "svc")
            .operation()
            .execution(call("/a"))
            .compensation(call("/b"))
            .add()
            .build()
            .getSteps()
            .get(0);
  }

  // --- undo must not reference its own forward output (compensation-failure-model) -------------

  @Test
  void build_compensationReferencesOwnExecutionOutput_throwsException() {
    // Arrange — the compensation references ${debitId}, which the execution itself produces. A
    // failed execution never binds that output, so the undo could not resolve it.
    HttpCall execution = HttpCall.newBuilder("/debit").output(Map.of("debitId", "$.id")).build();
    HttpCall compensation =
        HttpCall.newBuilder("/reverse").jsonBody(Map.of("id", "${debitId}")).build();
    SagaDefinition.Builder builder =
        SagaDefinition.newBuilder("transfer", SagaMode.SAGA)
            .serviceStep("debit", "account-service")
            .operation()
            .execution(execution)
            .compensation(compensation)
            .add();

    // Act & Assert
    assertThatThrownBy(builder::build).isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void build_cancellationReferencesOwnReservationOutput_throwsException() {
    // Arrange — the cancellation references ${reserveId}, which the reservation itself produces.
    HttpCall reservation =
        HttpCall.newBuilder("/reserve").output(Map.of("reserveId", "$.id")).build();
    HttpCall cancellation =
        HttpCall.newBuilder("/cancel").jsonBody(Map.of("id", "${reserveId}")).build();
    SagaDefinition.Builder builder =
        SagaDefinition.newBuilder("reserveSeats", SagaMode.TCC)
            .serviceStep("seat", "booking-service")
            .tccOperation()
            .reservation(reservation)
            .confirmation(call("/confirm"))
            .cancellation(cancellation)
            .add();

    // Act & Assert
    assertThatThrownBy(builder::build).isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void build_compensationReferencesInputNotOwnOutput_succeeds() {
    // Arrange — the compensation references ${accountId} (a saga input), not its own output.
    HttpCall execution = HttpCall.newBuilder("/debit").output(Map.of("debitId", "$.id")).build();
    HttpCall compensation =
        HttpCall.newBuilder("/reverse").jsonBody(Map.of("account", "${accountId}")).build();

    // Act
    SagaDefinition definition =
        SagaDefinition.newBuilder("transfer", SagaMode.SAGA)
            .serviceStep("debit", "account-service")
            .operation()
            .execution(execution)
            .compensation(compensation)
            .add()
            .build();

    // Assert
    assertThat(definition.getSteps()).hasSize(1);
  }

  @Test
  void build_compensationReferencesPriorStepOutput_succeeds() {
    // Arrange — step "credit"'s compensation references ${debitId}, produced by the PRIOR "debit"
    // step (durably available), not by "credit" itself. That is allowed.
    HttpCall debitExec = HttpCall.newBuilder("/debit").output(Map.of("debitId", "$.id")).build();
    HttpCall creditComp =
        HttpCall.newBuilder("/credit/reverse").jsonBody(Map.of("ref", "${debitId}")).build();

    // Act
    SagaDefinition definition =
        SagaDefinition.newBuilder("transfer", SagaMode.SAGA)
            .serviceStep("debit", "account-service")
            .operation()
            .execution(debitExec)
            .compensation(call("/debit/reverse"))
            .add()
            .serviceStep("credit", "account-service")
            .operation()
            .execution(call("/credit"))
            .compensation(creditComp)
            .add()
            .build();

    // Assert
    assertThat(definition.getSteps()).hasSize(2);
  }
}
