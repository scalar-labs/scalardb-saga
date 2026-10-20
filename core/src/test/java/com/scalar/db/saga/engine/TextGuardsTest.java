package com.scalar.db.saga.engine;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TextGuardsTest {

  @Test
  void oneLine_controlCharactersGiven_replacesEachWithASpace() {
    assertThat(TextGuards.oneLine("a\nb\r\tc\u0000d")).isEqualTo("a b  c d");
  }

  @Test
  void oneLine_plainTextGiven_returnsItUnchanged() {
    assertThat(TextGuards.oneLine("card declined")).isEqualTo("card declined");
  }

  @Test
  void cut_textWithinMaxGiven_returnsItWhole() {
    assertThat(TextGuards.cut("abc", 3)).isEqualTo("abc");
  }

  @Test
  void cut_textOverMaxGiven_returnsFirstMaxChars() {
    assertThat(TextGuards.cut("abcdef", 4)).isEqualTo("abcd");
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
