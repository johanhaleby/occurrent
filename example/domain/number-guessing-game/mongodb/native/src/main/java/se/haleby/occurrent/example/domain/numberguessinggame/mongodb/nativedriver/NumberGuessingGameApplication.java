package se.haleby.occurrent.example.domain.numberguessinggame.mongodb.nativedriver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.javalin.Javalin;
import j2html.tags.ContainerTag;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;
import se.haleby.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import se.haleby.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import se.haleby.occurrent.eventstore.mongodb.nativedriver.StreamConsistencyGuarantee;
import se.haleby.occurrent.example.domain.numberguessinggame.model.Guess;
import se.haleby.occurrent.example.domain.numberguessinggame.model.MaxNumberOfGuesses;
import se.haleby.occurrent.example.domain.numberguessinggame.model.NumberGuessingGame;
import se.haleby.occurrent.example.domain.numberguessinggame.model.SecretNumberToGuess;
import se.haleby.occurrent.example.domain.numberguessinggame.model.domainevents.*;
import se.haleby.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.projection.GameStatus;
import se.haleby.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.projection.WhatIsTheStatusOfGame;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static j2html.TagCreator.*;
import static java.util.Objects.requireNonNull;
import static se.haleby.occurrent.eventstore.api.Filter.subject;

public class NumberGuessingGameApplication {


    private static Javalin javalin;
    private final MongoEventStore mongoEventStore;

    public NumberGuessingGameApplication(MongoEventStore mongoEventStore) {
        this.mongoEventStore = mongoEventStore;
    }

    public static void main(String[] args) {
        NumberGuessingGameApplication application = NumberGuessingGameApplication.bootstrap();
        Runtime.getRuntime().addShutdownHook(new Thread(application::shutdown));
    }

    public static NumberGuessingGameApplication bootstrap() {
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoEventStore mongoEventStore = new MongoEventStore(mongoClient, "test", "events", new EventStoreConfig(StreamConsistencyGuarantee.transactional("versions"), TimeRepresentation.DATE));

        Serialization serialization = new Serialization(new ObjectMapper(), URI.create("urn:occurrent:domain:numberguessinggame"));
        NumberGuessingGameApplicationService numberGuessingGameApplicationService = new NumberGuessingGameApplicationService(mongoEventStore, serialization);

        javalin = Javalin.create(cfg -> cfg.showJavalinBanner = false).start(7000);

        WhatIsTheStatusOfGame whatIsTheStatusOfGame = gameId -> mongoEventStore.query(subject(gameId.toString()))
                .map(serialization::deserialize)
                .collect(GameStatusBuilder::new,
                        (gameStatus, event) -> {
                            if (event instanceof NumberGuessingGameWasStarted) {
                                gameStatus.gameId = event.gameId();
                                gameStatus.secretNumber = ((NumberGuessingGameWasStarted) event).secretNumberToGuess();
                                gameStatus.maxNumberOfGuesses = ((NumberGuessingGameWasStarted) event).maxNumberOfGuesses();
                            } else if (event instanceof PlayerGuessedANumberThatWasTooSmall) {
                                gameStatus.guesses.add(((PlayerGuessedANumberThatWasTooSmall) event).guessedNumber());
                            } else if (event instanceof PlayerGuessedANumberThatWasTooBig) {
                                gameStatus.guesses.add(((PlayerGuessedANumberThatWasTooBig) event).guessedNumber());
                            } else if (event instanceof PlayerGuessedTheRightNumber) {
                                gameStatus.guesses.add(((PlayerGuessedTheRightNumber) event).guessedNumber());
                            }
                        }, (gameStatus, gameStatus2) -> {
                        })
                .build();

        configureRoutes(numberGuessingGameApplicationService, whatIsTheStatusOfGame, 1, 20, MaxNumberOfGuesses.of(5));


        return new NumberGuessingGameApplication(mongoEventStore);
    }

    @SuppressWarnings("SameParameterValue")
    private static void configureRoutes(NumberGuessingGameApplicationService as, WhatIsTheStatusOfGame whatIsTheStatusOfGame, int minNumberToGuess, int maxNumberToGuess, MaxNumberOfGuesses maxNumberOfGuesses) {
        javalin.get("/", ctx -> {
            String body = body(
                    h1("Number Guessing Game"),
                    form().withMethod("post").withAction("/games").with(
                            input().withName("gameId").withType("hidden").withValue(UUID.randomUUID().toString()),
                            input().withName("playerId").withType("hidden").withValue(UUID.randomUUID().toString()),
                            button("Start new game").withType("submit")
                    )
            ).render();

            ctx.html(body);
        });

        javalin.post("/games", ctx -> {
            UUID gameId = UUID.fromString(requireNonNull(ctx.formParam("gameId")));
            UUID playerId = UUID.fromString(requireNonNull(ctx.formParam("playerId")));
            List<GameEvent> events = as.play(gameId, __ -> NumberGuessingGame.startNewGame(gameId, LocalDateTime.now(), playerId, SecretNumberToGuess.randomBetween(minNumberToGuess, maxNumberToGuess), maxNumberOfGuesses));

            final String message;
            if (contains(events, NumberGuessingGameWasStarted.class)) {
                message = String.format("Game started! It's time to guess a number between %d and %d.", minNumberToGuess, maxNumberToGuess);
            } else {
                message = "Something unexpected happened";
            }
            String body = body(h1(message), generateGuessForm(gameId, playerId, minNumberToGuess, maxNumberToGuess, 1)).render();
            ctx.html(body);
        });

        javalin.post("/games/:gameId", ctx -> {
            UUID gameId = UUID.fromString(requireNonNull(ctx.pathParam("gameId")));
            UUID playerId = UUID.fromString(requireNonNull(ctx.formParam("playerId")));
            int guess = Integer.parseInt(requireNonNull(ctx.formParam("guess")));
            List<GameEvent> events = as.play(gameId, state -> NumberGuessingGame.guessNumber(state, gameId, LocalDateTime.now(), playerId, new Guess(guess)));

            final String message;
            if (contains(events, GuessingAttemptsExhausted.class)) {
                GameStatus gameStatus = whatIsTheStatusOfGame.findFor(gameId).orElseThrow(IllegalStateException::new);
                message = String.format("Sorry, you failed to guess the number in %d attempts. Number was %d.", gameStatus.maxNumberOfGuesses, gameStatus.secretNumber);
            } else if (contains(events, PlayerGuessedANumberThatWasTooSmall.class)) {
                PlayerGuessedANumberThatWasTooSmall event = findEvent(events, PlayerGuessedANumberThatWasTooSmall.class);
                message = String.format("%d is too small, try again", event.guessedNumber());
            } else if (contains(events, PlayerGuessedANumberThatWasTooBig.class)) {
                PlayerGuessedANumberThatWasTooBig event = findEvent(events, PlayerGuessedANumberThatWasTooBig.class);
                message = String.format("%d is too big, try again", event.guessedNumber());
            } else if (contains(events, PlayerGuessedTheRightNumber.class)) {
                PlayerGuessedTheRightNumber event = findEvent(events, PlayerGuessedTheRightNumber.class);
                message = String.format("%d is the right number, great work, you won! :)", event.guessedNumber());
            } else {
                message = "Something unexpected happened";
            }

            final ContainerTag form;
            if (contains(events, NumberGuessingGameEnded.class)) {
                form = button("Play again").attr("onclick", "window.location.href='/';");
            } else {
                form = generateGuessForm(gameId, playerId, minNumberToGuess, maxNumberToGuess, guess);
            }

            String body = body(h1(message), form).render();

            ctx.html(body);
        });
    }

    private static ContainerTag generateGuessForm(UUID gameId, UUID playerId, int minNumberToGuess, int maxNumberToGuess, int value) {
        return form().withMethod("post").withAction("/games/" + gameId.toString()).with(
                input().withName("playerId").withType("hidden").withValue(playerId.toString()),
                input().withName("guess").withType("number").attr("min", minNumberToGuess).attr("max", maxNumberToGuess).withValue(String.valueOf(value)),
                button("Make guess").withType("submit"));
    }

    private static boolean contains(List<GameEvent> events, Class<? extends GameEvent> cls) {
        return events.stream().anyMatch(cls::isInstance);
    }

    private static <T extends GameEvent> T findEvent(List<GameEvent> events, Class<T> cls) {
        return events.stream().filter(cls::isInstance).findFirst().map(cls::cast).orElse(null);
    }

    public void shutdown() {
        javalin.stop();
        mongoEventStore.shutdown();
    }

    private static class GameStatusBuilder {
        public UUID gameId;
        public int maxNumberOfGuesses;
        public int secretNumber;
        public List<Integer> guesses = new ArrayList<>();

        private Optional<GameStatus> build() {
            return gameId == null ? Optional.empty() : Optional.of(new GameStatus(gameId, secretNumber, maxNumberOfGuesses, guesses));
        }
    }
}