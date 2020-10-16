package org.occurrent.example.domain.wordguessinggame.writemodel

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.*
import org.occurrent.example.domain.wordguessinggame.event.*
import org.occurrent.example.domain.wordguessinggame.writemodel.*
import java.util.*

@DisplayName("game specification")
class GameLogicKtTest {

    @Nested
    @DisplayName("when game is not started")
    inner class NotStarted {

        @Nested
        @DisplayName("and no previous events exists for game")
        inner class AndNoPreviousEventsExistsForGame {

            @Test
            fun `then startGame returns GameWasStarted event`() {
                // Given
                val currentEvents = emptySequence<DomainEvent>()

                val gameId = GameId.randomUUID()
                val timestamp = Timestamp()
                val playerId = PlayerId.randomUUID()
                val wordsToChooseFrom = WordsToChooseFrom(WordCategory("Category"), wordsOf("word1", "word2", "word3", "word4", "word5"))

                // When
                val newEvents = startGame(currentEvents, gameId, timestamp, playerId, wordsToChooseFrom, MaxNumberOfGuessesPerPlayer, MaxNumberOfGuessesTotal)

                // Then
                val events = newEvents.toList()
                assertThat(events).hasSize(1)
                assertThat(events[0]).isExactlyInstanceOf(GameWasStarted::class.java)
                val event = events[0] as GameWasStarted
                assertAll(
                        { assertThat(event.gameId).isEqualTo(gameId) },
                        { assertThat(event.timestamp).isEqualTo(timestamp) },
                        { assertThat(event.category).isEqualTo("Category") },
                        { assertThat(event.maxNumberOfGuessesPerPlayer).isEqualTo(MaxNumberOfGuessesPerPlayer.value) },
                        { assertThat(event.maxNumberOfGuessesTotal).isEqualTo(MaxNumberOfGuessesTotal.value) },
                        { assertThat(event.wordToGuess).isIn("word1", "word2", "word3", "word4", "word5") },
                )
            }
        }


        @Nested
        @DisplayName("and previous events exists for game")
        inner class AndPreviousEventsExistsForGame {

            @Test
            fun `then startGame returns throws illegal state exception`() {
                // Given
                val gameId = GameId.randomUUID()
                val currentEvents = sequenceOf(GameWasStarted(UUID.randomUUID(), Timestamp(), gameId, PlayerId.randomUUID(), "Category", "Word", 2, 4))

                val wordsToChooseFrom = WordsToChooseFrom(WordCategory("Category"), wordsOf("word1", "word2", "word3", "word4", "word5"))

                // When
                val throwable = catchThrowable {
                    startGame(currentEvents, gameId, Timestamp(), PlayerId.randomUUID(), wordsToChooseFrom, MaxNumberOfGuessesPerPlayer, MaxNumberOfGuessesTotal)
                }

                // Then
                assertThat(throwable).isExactlyInstanceOf(IllegalStateException::class.java).hasMessage("Cannot start game $gameId since it has already been started")
            }
        }
    }


    @Nested
    @DisplayName("when game is started")
    inner class WhenGameIsStarted {

        lateinit var gameId: GameId


        @BeforeEach
        fun `game id is generated`() {
            gameId = GameId.randomUUID()
        }

        @Nested
        @DisplayName("and player guess the right word")
        inner class AndPlayerGuessTheRightWord {

            private lateinit var currentEvents: Sequence<DomainEvent>

            @BeforeEach
            fun `game is started`() {
                currentEvents = sequenceOf(GameWasStarted(UUID.randomUUID(), Timestamp(), gameId, PlayerId.randomUUID(), "Category", "Secret", 2, 4))
            }

            @Test
            fun `then guessWord returns PlayerGuessedTheRightWord and GameWasWon`() {
                // Given
                val playerId = PlayerId.randomUUID()
                val timestamp = Timestamp()

                // When
                val events = guessWord(currentEvents, timestamp, playerId, Word("Secret")).toList()

                // Then
                assertThat(events).hasSize(2)
                val playerGuessedTheRightWord = events.find<PlayerGuessedTheRightWord>()
                val gameWon = events.find<GameWasWon>()

                assertAll(
                        // PlayerGuessedTheRightWord
                        { assertThat(playerGuessedTheRightWord.gameId).isEqualTo(gameId) },
                        { assertThat(playerGuessedTheRightWord.playerId).isEqualTo(playerId) },
                        { assertThat(playerGuessedTheRightWord.timestamp).isEqualTo(timestamp) },
                        { assertThat(playerGuessedTheRightWord.guessedWord).isEqualTo("Secret") },
                        { assertThat(playerGuessedTheRightWord.type).isEqualTo(PlayerGuessedTheRightWord::class.simpleName) },
                        // GameWasWon
                        { assertThat(gameWon.gameId).isEqualTo(gameId) },
                        { assertThat(gameWon.winnerId).isEqualTo(playerId) },
                        { assertThat(gameWon.timestamp).isEqualTo(timestamp) },
                        { assertThat(gameWon.type).isEqualTo(GameWasWon::class.simpleName) },
                )
            }
        }

        @Nested
        @DisplayName("and player guess the right word with different case")
        inner class AndPlayerGuessTheRightWordWithDifferentCase {

            private lateinit var currentEvents: Sequence<DomainEvent>

            @BeforeEach
            fun `game is started`() {
                currentEvents = sequenceOf(GameWasStarted(UUID.randomUUID(), Timestamp(), gameId, PlayerId.randomUUID(), "Category", "Secret", 2, 4))
            }

            @Test
            fun `then guessWord returns PlayerGuessedTheRightWord and GameWasWon`() {
                // Given
                val playerId = PlayerId.randomUUID()
                val timestamp = Timestamp()

                // When
                val events = guessWord(currentEvents, timestamp, playerId, Word("secret")).toList()

                // Then
                assertThat(events).hasSize(2)
                val playerGuessedTheRightWord = events.find<PlayerGuessedTheRightWord>()
                val gameWon = events.find<GameWasWon>()

                assertAll(
                        // PlayerGuessedTheRightWord
                        { assertThat(playerGuessedTheRightWord.gameId).isEqualTo(gameId) },
                        { assertThat(playerGuessedTheRightWord.playerId).isEqualTo(playerId) },
                        { assertThat(playerGuessedTheRightWord.timestamp).isEqualTo(timestamp) },
                        { assertThat(playerGuessedTheRightWord.guessedWord).isEqualTo("secret") },
                        { assertThat(playerGuessedTheRightWord.type).isEqualTo(PlayerGuessedTheRightWord::class.simpleName) },
                        // GameWasWon
                        { assertThat(gameWon.gameId).isEqualTo(gameId) },
                        { assertThat(gameWon.winnerId).isEqualTo(playerId) },
                        { assertThat(gameWon.timestamp).isEqualTo(timestamp) },
                        { assertThat(gameWon.type).isEqualTo(GameWasWon::class.simpleName) },
                )
            }
        }

        @Nested
        @DisplayName("and player guess the wrong word")
        inner class AndPlayerGuessTheWrongWord {


            @Nested
            @DisplayName("when player has more guesses left")
            inner class WhenPlayerHasMoreGuessesLeft {

                private lateinit var currentEvents: Sequence<DomainEvent>

                @BeforeEach
                fun `game is started`() {
                    currentEvents = sequenceOf(GameWasStarted(UUID.randomUUID(), Timestamp(), gameId, PlayerId.randomUUID(), "Category", "Secret", 2, 4))
                }

                @Test
                fun `then PlayerGuessTheWrongWord is returned`() {
                    // Given
                    val playerId = PlayerId.randomUUID()
                    val timestamp = Timestamp()

                    // When
                    val events = guessWord(currentEvents, timestamp, playerId, Word("Wrong Word")).toList()

                    // Then
                    assertThat(events).hasSize(1)
                    val playerGuessedTheWrongWord = events.find<PlayerGuessedTheWrongWord>()

                    assertAll(
                            // PlayerGuessedTheWrongWord
                            { assertThat(playerGuessedTheWrongWord.gameId).isEqualTo(gameId) },
                            { assertThat(playerGuessedTheWrongWord.playerId).isEqualTo(playerId) },
                            { assertThat(playerGuessedTheWrongWord.timestamp).isEqualTo(timestamp) },
                            { assertThat(playerGuessedTheWrongWord.guessedWord).isEqualTo("Wrong Word") },
                            { assertThat(playerGuessedTheWrongWord.type).isEqualTo(PlayerGuessedTheWrongWord::class.simpleName) },
                    )
                }
            }

            @Nested
            @DisplayName("when last guess for player but more guesses remaining in game")
            inner class WhenLastGuessForPlayer {

                private lateinit var currentEvents: Sequence<DomainEvent>
                private lateinit var playerId: PlayerId

                @BeforeEach
                fun `game is started`() {
                    playerId = PlayerId.randomUUID()

                    currentEvents = sequenceOf(
                            GameWasStarted(UUID.randomUUID(), Timestamp(), gameId, playerId, "Category", "Secret", 2, 4),
                            PlayerGuessedTheWrongWord(UUID.randomUUID(), Timestamp(), gameId, playerId, "Wrong"))
                }

                @Test
                fun `then PlayerGuessTheWrongWord and NumberOfGuessesWasExhaustedForPlayer is returned`() {
                    // Given
                    val timestamp = Timestamp()

                    // When
                    val events = guessWord(currentEvents, timestamp, playerId, Word("Wrong Word")).toList()

                    // Then
                    assertThat(events).hasSize(2)
                    val playerGuessedTheWrongWord = events.find<PlayerGuessedTheWrongWord>()
                    val numberOfGuessesWasExhaustedForPlayer = events.find<NumberOfGuessesWasExhaustedForPlayer>()

                    assertAll(
                            // PlayerGuessedTheWrongWord
                            { assertThat(playerGuessedTheWrongWord.gameId).isEqualTo(gameId) },
                            { assertThat(playerGuessedTheWrongWord.playerId).isEqualTo(playerId) },
                            { assertThat(playerGuessedTheWrongWord.timestamp).isEqualTo(timestamp) },
                            { assertThat(playerGuessedTheWrongWord.guessedWord).isEqualTo("Wrong Word") },
                            { assertThat(playerGuessedTheWrongWord.type).isEqualTo(PlayerGuessedTheWrongWord::class.simpleName) },
                            // NumberOfGuessesWasExhaustedForPlayer
                            { assertThat(numberOfGuessesWasExhaustedForPlayer.gameId).isEqualTo(gameId) },
                            { assertThat(numberOfGuessesWasExhaustedForPlayer.timestamp).isEqualTo(timestamp) },
                            { assertThat(numberOfGuessesWasExhaustedForPlayer.playerId).isEqualTo(playerId) },
                            { assertThat(numberOfGuessesWasExhaustedForPlayer.type).isEqualTo(NumberOfGuessesWasExhaustedForPlayer::class.simpleName) },
                    )
                }
            }

            @Nested
            @DisplayName("when player tries to guess but attempts are already exhausted")
            inner class WhenPlayerTriesToGuessButAttemptsAreAlreadyExhausted {

                private lateinit var currentEvents: Sequence<DomainEvent>
                private lateinit var playerId: PlayerId

                @BeforeEach
                fun `game is started`() {
                    playerId = PlayerId.randomUUID()

                    currentEvents = sequenceOf(
                            GameWasStarted(UUID.randomUUID(), Timestamp(), gameId, playerId, "Category", "Secret", 2, 4),
                            PlayerGuessedTheWrongWord(UUID.randomUUID(), Timestamp(), gameId, playerId, "Wrong1"),
                            PlayerGuessedTheWrongWord(UUID.randomUUID(), Timestamp(), gameId, playerId, "Wrong2"))
                }

                @Test
                fun `then PlayerGuessTheWrongWord is returned`() {
                    // Given
                    val timestamp = Timestamp()

                    // When
                    val throwable = catchThrowable { guessWord(currentEvents, timestamp, playerId, Word("Wrong Word")) }

                    // Then
                    assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException::class.java).hasMessage("Number of guessing attempts exhausted for player $playerId.")
                }
            }

            @Nested
            @DisplayName("when last guess for game")
            inner class WhenLastGuessForGame {

                private lateinit var currentEvents: Sequence<DomainEvent>
                private lateinit var playerId1: PlayerId
                private lateinit var playerId2: PlayerId

                @BeforeEach
                fun `game is started`() {
                    playerId1 = PlayerId.randomUUID()
                    playerId2 = PlayerId.randomUUID()

                    currentEvents = sequenceOf(
                            GameWasStarted(UUID.randomUUID(), Timestamp(), gameId, playerId1, "Category", "Secret", 2, 3),
                            PlayerGuessedTheWrongWord(UUID.randomUUID(), Timestamp(), gameId, playerId1, "Wrong1"),
                            PlayerGuessedTheWrongWord(UUID.randomUUID(), Timestamp(), gameId, playerId1, "Wrong2"))
                }

                @Test
                fun `then PlayerGuessTheWrongWord and GameWasLost is returned`() {
                    // Given
                    val timestamp = Timestamp()

                    // When
                    val events = guessWord(currentEvents, timestamp, playerId2, Word("Wrong Word")).toList()

                    // Then
                    assertThat(events).hasSize(2)
                    val playerGuessedTheWrongWord = events.find<PlayerGuessedTheWrongWord>()
                    val gameWasLost = events.find<GameWasLost>()

                    assertAll(
                            // PlayerGuessedTheWrongWord
                            { assertThat(playerGuessedTheWrongWord.gameId).isEqualTo(gameId) },
                            { assertThat(playerGuessedTheWrongWord.playerId).isEqualTo(playerId2) },
                            { assertThat(playerGuessedTheWrongWord.timestamp).isEqualTo(timestamp) },
                            { assertThat(playerGuessedTheWrongWord.guessedWord).isEqualTo("Wrong Word") },
                            { assertThat(playerGuessedTheWrongWord.type).isEqualTo(PlayerGuessedTheWrongWord::class.simpleName) },
                            // GameWasLost
                            { assertThat(gameWasLost.gameId).isEqualTo(gameId) },
                            { assertThat(gameWasLost.timestamp).isEqualTo(timestamp) },
                            { assertThat(gameWasLost.type).isEqualTo(GameWasLost::class.simpleName) },
                    )
                }
            }
        }
    }

    @Nested
    @DisplayName("when game is lost")
    inner class WhenGameIsLost {

        @Test
        fun `then it's not possible for any player to make a guess`() {
            // Given
            val gameId = GameId.randomUUID()
            val playerId = PlayerId.randomUUID()
            val timestamp = Timestamp()

            val currentEvents = sequenceOf(GameWasStarted(UUID.randomUUID(), Timestamp(), gameId, playerId, "Category", "Secret", 2, 1),
                    PlayerGuessedTheWrongWord(UUID.randomUUID(), Timestamp(), gameId, playerId, "Wrong"),
                    GameWasLost(UUID.randomUUID(), Timestamp(), gameId))

            // When
            val throwable = catchThrowable { guessWord(currentEvents, timestamp, playerId, Word("Secret")) }

            // Then
            assertThat(throwable).isExactlyInstanceOf(IllegalStateException::class.java).hasMessage("Cannot guess word for a game that is already ended")
        }
    }

    @Nested
    @DisplayName("when game is won")
    inner class WhenGameIsWon {

        @Test
        fun `then it's not possible for any player to make a guess`() {
            // Given
            val gameId = GameId.randomUUID()
            val playerId = PlayerId.randomUUID()
            val timestamp = Timestamp()

            val currentEvents = sequenceOf(GameWasStarted(UUID.randomUUID(), Timestamp(), gameId, playerId, "Category", "Secret", 2, 4),
                    PlayerGuessedTheRightWord(UUID.randomUUID(), Timestamp(), gameId, playerId, "Secret"),
                    GameWasWon(UUID.randomUUID(), Timestamp(), gameId, winnerId = playerId))

            // When
            val throwable = catchThrowable { guessWord(currentEvents, timestamp, playerId, Word("Secret")) }

            // Then
            assertThat(throwable).isExactlyInstanceOf(IllegalStateException::class.java).hasMessage("Cannot guess word for a game that is already ended")
        }
    }
}

private fun wordsOf(vararg words: String) = listOf(*words).map(::Word)

private inline fun <reified T : DomainEvent> List<DomainEvent>.find(): T = first { it is T } as T