package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.saga.api.SagaContext;
import com.scalar.db.saga.api.Step;
import com.scalar.db.saga.api.StepResult;
import com.scalar.db.saga.api.TccStep;
import com.scalar.db.saga.definition.HttpCall;
import com.scalar.db.saga.definition.SagaDefinition;
import com.scalar.db.saga.definition.SagaDefinition.StepDefinition;
import com.scalar.db.saga.exception.SagaDefinitionException;
import com.scalar.db.saga.transport.HttpEndpointManager;
import com.scalar.db.saga.transport.HttpServiceConfig;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StepInstantiatorTest {

  private static final HttpEndpointManager EMPTY_ENDPOINTS = HttpEndpointManager.create(Map.of());

  @Test
  void instantiate_classStep_resolvesViaStepResolver() {
    // Arrange
    Step resolved = noopStep("debit");
    StepInstantiator instantiator =
        new StepInstantiator((name, className, ctx) -> resolved, EMPTY_ENDPOINTS);

    // Act
    Step step = instantiator.instantiate(classStep("debit", "com.example.DebitStep"), Step.class);

    // Assert
    assertThat(step).isSameAs(resolved);
  }

  @Test
  void instantiate_classStepNotExpectedType_throwsSagaDefinitionException() {
    // Arrange — resolver returns a Step, but the TCC path expects a TccStep
    StepInstantiator instantiator =
        new StepInstantiator((name, className, ctx) -> noopStep("x"), EMPTY_ENDPOINTS);

    // Act & Assert
    assertThatThrownBy(
            () -> instantiator.instantiate(classStep("x", "com.example.X"), TccStep.class))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void instantiate_declarativeSagaStep_resolvesToStep() {
    // Arrange — an HTTP endpoint is registered for the step's service
    HttpEndpointManager endpoints =
        HttpEndpointManager.create(
            Map.of(
                "account",
                new HttpServiceConfig("http://account-svc:8080", List.of(), -1, null, Map.of())));
    StepInstantiator instantiator =
        new StepInstantiator((name, className, ctx) -> noopStep(name), endpoints);

    // Act
    Step step = instantiator.instantiate(declarativeSagaStep("debit", "account"), Step.class);

    // Assert
    assertThat(step.getName()).isEqualTo("debit");
  }

  @Test
  void instantiate_declarativeTccStep_resolvesToTccStep() {
    // Arrange
    HttpEndpointManager endpoints =
        HttpEndpointManager.create(
            Map.of(
                "booking",
                new HttpServiceConfig("http://booking-svc:8080", List.of(), -1, null, Map.of())));
    StepInstantiator instantiator =
        new StepInstantiator((name, className, ctx) -> noopStep(name), endpoints);

    // Act
    TccStep step = instantiator.instantiate(declarativeTccStep("seat", "booking"), TccStep.class);

    // Assert
    assertThat(step.getName()).isEqualTo("seat");
  }

  @Test
  void instantiate_declarativeStepNotExpectedType_throwsSagaDefinitionException() {
    // Arrange — the declarative adapter produces a Step, but an unexpected type is requested
    HttpEndpointManager endpoints =
        HttpEndpointManager.create(
            Map.of(
                "account",
                new HttpServiceConfig("http://account-svc:8080", List.of(), -1, null, Map.of())));
    StepInstantiator instantiator =
        new StepInstantiator((name, className, ctx) -> noopStep(name), endpoints);

    // Act & Assert — a wrong expected type must surface as SagaDefinitionException, not CCE
    assertThatThrownBy(
            () -> instantiator.instantiate(declarativeSagaStep("debit", "account"), Runnable.class))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void instantiate_declarativeStepUnregisteredService_throwsSagaDefinitionException() {
    // Arrange
    StepInstantiator instantiator =
        new StepInstantiator((name, className, ctx) -> noopStep(name), EMPTY_ENDPOINTS);

    // Act & Assert
    assertThatThrownBy(
            () -> instantiator.instantiate(declarativeSagaStep("debit", "missing"), Step.class))
        .isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void httpClient_soleEndpointRegistered_returnsClient() {
    // Arrange — exactly one endpoint, so the unnamed lookup is unambiguous
    HttpEndpointManager endpoints =
        HttpEndpointManager.create(
            Map.of(
                "account",
                new HttpServiceConfig("http://account-svc:8080", List.of(), -1, null, Map.of())));
    StepInstantiator instantiator =
        new StepInstantiator((name, className, ctx) -> noopStep(name), endpoints);

    // Act & Assert
    assertThat(instantiator.httpClient()).isNotNull();
  }

  @Test
  void httpClient_noEndpointRegistered_throwsSagaDefinitionException() {
    // Arrange
    StepInstantiator instantiator =
        new StepInstantiator((name, className, ctx) -> noopStep(name), EMPTY_ENDPOINTS);

    // Act & Assert
    assertThatThrownBy(instantiator::httpClient).isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void httpClient_multipleEndpointsRegistered_throwsSagaDefinitionException() {
    // Arrange — with two endpoints the unnamed lookup is ambiguous; @Named must select one
    HttpEndpointManager endpoints =
        HttpEndpointManager.create(
            Map.of(
                "account",
                new HttpServiceConfig("http://account-svc:8080", List.of(), -1, null, Map.of()),
                "payment",
                new HttpServiceConfig("http://payment-svc:8080", List.of(), -1, null, Map.of())));
    StepInstantiator instantiator =
        new StepInstantiator((name, className, ctx) -> noopStep(name), endpoints);

    // Act & Assert
    assertThatThrownBy(instantiator::httpClient).isInstanceOf(SagaDefinitionException.class);
  }

  @Test
  void httpClient_registeredNameGiven_returnsClient() {
    // Arrange
    HttpEndpointManager endpoints =
        HttpEndpointManager.create(
            Map.of(
                "account",
                new HttpServiceConfig("http://account-svc:8080", List.of(), -1, null, Map.of())));
    StepInstantiator instantiator =
        new StepInstantiator((name, className, ctx) -> noopStep(name), endpoints);

    // Act & Assert
    assertThat(instantiator.httpClient("account")).isNotNull();
  }

  @Test
  void httpClient_unknownNameGiven_throwsSagaDefinitionException() {
    // Arrange
    HttpEndpointManager endpoints =
        HttpEndpointManager.create(
            Map.of(
                "account",
                new HttpServiceConfig("http://account-svc:8080", List.of(), -1, null, Map.of())));
    StepInstantiator instantiator =
        new StepInstantiator((name, className, ctx) -> noopStep(name), endpoints);

    // Act & Assert
    assertThatThrownBy(() -> instantiator.httpClient("payment"))
        .isInstanceOf(SagaDefinitionException.class);
  }

  private static StepDefinition declarativeSagaStep(String name, String service) {
    return SagaDefinition.newBuilder("s")
        .saga()
        .serviceStep(name, service)
        .execution(HttpCall.newBuilder("/do").build())
        .compensation(HttpCall.newBuilder("/undo").build())
        .add()
        .build()
        .getSteps()
        .get(0);
  }

  private static StepDefinition declarativeTccStep(String name, String service) {
    return SagaDefinition.newBuilder("s")
        .tcc()
        .serviceStep(name, service)
        .reservation(HttpCall.newBuilder("/reserve").build())
        .confirmation(HttpCall.newBuilder("/confirm").build())
        .cancellation(HttpCall.newBuilder("/cancel").build())
        .add()
        .build()
        .getSteps()
        .get(0);
  }

  private static StepDefinition classStep(String name, String stepClass) {
    return SagaDefinition.newBuilder("s")
        .saga()
        .step(name, stepClass)
        .add()
        .build()
        .getSteps()
        .get(0);
  }

  private static Step noopStep(String name) {
    return new Step() {
      @Override
      public String getName() {
        return name;
      }

      @Override
      public StepResult execute(SagaContext context) {
        return StepResult.empty();
      }

      @Override
      public void compensate(SagaContext context) {}
    };
  }
}
