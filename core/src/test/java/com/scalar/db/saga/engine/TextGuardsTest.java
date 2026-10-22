package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TextGuardsTest {

  @Test
  void oneLine_controlCharactersGiven_replacesEachWithASpace() {
    // Arrange
    String text = "a\nb\r\tc\u0000d";

    // Act
    String line = TextGuards.oneLine(text);

    // Assert
    assertThat(line).isEqualTo("a b  c d");
  }

  @Test
  void oneLine_plainTextGiven_returnsItUnchanged() {
    // Arrange
    String text = "card declined";

    // Act
    String line = TextGuards.oneLine(text);

    // Assert
    assertThat(line).isEqualTo("card declined");
  }

  @Test
  void cut_textWithinMaxGiven_returnsItWhole() {
    // Arrange
    String text = "abc";

    // Act
    String cut = TextGuards.cut(text, 3);

    // Assert
    assertThat(cut).isEqualTo("abc");
  }

  @Test
  void cut_textOverMaxGiven_returnsFirstMaxChars() {
    // Arrange
    String text = "abcdef";

    // Act
    String cut = TextGuards.cut(text, 4);

    // Assert
    assertThat(cut).isEqualTo("abcd");
  }

  @Test
  void cut_boundaryInsideASurrogatePair_backsOffOneChar() {
    // Arrange — "ab" then U+1F600, which takes two chars; a cut at 3 would split it.
    String text = "ab😀";

    // Act
    String cut = TextGuards.cut(text, 3);

    // Assert
    assertThat(cut).isEqualTo("ab");
  }
}
