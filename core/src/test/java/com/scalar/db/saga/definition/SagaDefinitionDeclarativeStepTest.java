package com.scalar.db.saga.definition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.definition.SagaDefinition.ServiceStep;
import com.scalar.db.saga.definition.SagaDefinition.ServiceStep.Phase;
import com.scalar.db.saga.definition.SagaDefinition.StepDefinition;
import com.scalar.db.saga.exception.SagaDefinitionException;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SagaDefinitionDeclarativeStepTest {

  private static HttpCall call(String path) {
    return HttpCall.newBuilder(path).build();
  }

  @Test
  void declarativeStep_sagaPhasesGiven_buildsSagaDeclarativeStep() {
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
        SagaDefinition.newBuilder("transfer")
            .saga()
            .serviceStep("debit", "account-service")
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
  void declarativeStep_tccPhasesGiven_buildsTccDeclarativeStep() {
    // Act
    SagaDefinition definition =
        SagaDefinition.newBuilder("reserveSeats")
            .tcc()
            .serviceStep("seat", "booking-service")
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
  void add_incompleteSagaPhases_throwsException() {
    // Arrange — execution without compensation.
    SagaDefinition.DeclarativeStepBuilder stepBuilder =
        SagaDefinition.newBuilder("saga")
            .saga()
            .serviceStep("debit", "account-service")
            .execution(call("/debit"));

    // Act & Assert — add() rejects the incomplete step.
    assertThatThrownBy(stepBuilder::add).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void add_incompleteTccPhases_throwsException() {
    // Arrange — reservation and confirmation without cancellation.
    SagaDefinition.TccDeclarativeStepBuilder stepBuilder =
        SagaDefinition.newBuilder("tcc")
            .tcc()
            .serviceStep("seat", "booking-service")
            .reservation(call("/reserve"))
            .confirmation(call("/confirm"));

    // Act & Assert — add() rejects the incomplete step.
    assertThatThrownBy(stepBuilder::add).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void execution_calledTwice_throwsException() {
    // Arrange
    SagaDefinition.DeclarativeStepBuilder stepBuilder =
        SagaDefinition.newBuilder("saga")
            .saga()
            .serviceStep("debit", "account-service")
            .execution(call("/debit"));

    // Act & Assert — setting the same phase twice is rejected.
    assertThatThrownBy(() -> stepBuilder.execution(call("/debit-again")))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void reservation_calledTwice_throwsException() {
    // Arrange
    SagaDefinition.TccDeclarativeStepBuilder stepBuilder =
        SagaDefinition.newBuilder("tcc")
            .tcc()
            .serviceStep("seat", "booking-service")
            .reservation(call("/reserve"));

    // Act & Assert — setting the same phase twice is rejected.
    assertThatThrownBy(() -> stepBuilder.reservation(call("/reserve-again")))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void build_mixedStepKindsGiven_succeeds() {
    // Act
    SagaDefinition definition =
        SagaDefinition.newBuilder("mixed")
            .saga()
            .step("classStep", "com.example.ClassStep")
            .add()
            .serviceStep("declStep", "account-service")
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
  void declarativeStep_blankNameGiven_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> SagaDefinition.newBuilder("s").saga().serviceStep(" ", "svc"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void declarativeStep_blankServiceGiven_throwsException() {
    // Act & Assert
    assertThatThrownBy(() -> SagaDefinition.newBuilder("s").saga().serviceStep("step", " "))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @SuppressWarnings("NullAway")
  @Test
  void execution_nullCallGiven_throwsException() {
    // Act & Assert
    assertThatThrownBy(
            () -> SagaDefinition.newBuilder("s").saga().serviceStep("step", "svc").execution(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void declarativeStep_equalSteps_areEqual() {
    // Arrange
    ServiceStep a = buildSagaStep();
    ServiceStep b = buildSagaStep();

    // Act & Assert
    assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
  }

  private static ServiceStep buildSagaStep() {
    return (ServiceStep)
        SagaDefinition.newBuilder("s")
            .saga()
            .serviceStep("step", "svc")
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
    var builder =
        SagaDefinition.newBuilder("transfer")
            .saga()
            .serviceStep("debit", "account-service")
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
    var builder =
        SagaDefinition.newBuilder("reserveSeats")
            .tcc()
            .serviceStep("seat", "booking-service")
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
        SagaDefinition.newBuilder("transfer")
            .saga()
            .serviceStep("debit", "account-service")
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
        SagaDefinition.newBuilder("transfer")
            .saga()
            .serviceStep("debit", "account-service")
            .execution(debitExec)
            .compensation(call("/debit/reverse"))
            .add()
            .serviceStep("credit", "account-service")
            .execution(call("/credit"))
            .compensation(creditComp)
            .add()
            .build();

    // Assert
    assertThat(definition.getSteps()).hasSize(2);
  }
}
