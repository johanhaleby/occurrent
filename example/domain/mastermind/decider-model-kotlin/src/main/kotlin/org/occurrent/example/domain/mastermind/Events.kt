import org.occurrent.example.domain.mastermind.*

sealed interface MasterMindEvent {
    val gameId: GameId
    val timestamp: Timestamp
}

data class GameStarted(
    override val gameId: GameId,
    override val timestamp: Timestamp,
    val codeBreakerId: CodebreakerId,
    val codeMakerId: CodeMakerId,
    val secretCode: SecretCode,
    val maxNumberOfGuesses: MaxNumberOfGuesses
) : MasterMindEvent

data class GuessMade(override val gameId: GameId, override val timestamp: Timestamp, val guess: Guess, val feedback: Feedback) : MasterMindEvent
data class GameWon(override val gameId: GameId, override val timestamp: Timestamp) : MasterMindEvent
data class GameLost(override val gameId: GameId, override val timestamp: Timestamp) : MasterMindEvent