/*
 * Copyright 2026 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.eventstore.mongodb.dcb.internal;

import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@DisplayName("DcbTagsIndexCheck")
@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbTagsIndexCheckTest {

    private static final String COLLECTION = "events";
    private static final String PLANNER_NOTE = "The planner on MongoDB 8.0.34, 7.0.45 and 4.2.8 uses the single and double $and forms, and 4.2.8 ignores a nested one";

    @Test
    void a_sparse_index_on_dcb_tags_alone_needs_no_warning() {
        Document index = dcbTagsIndex().append("sparse", true);

        assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index))).isNull();
    }

    @Test
    void a_sparse_flag_that_listIndexes_returns_as_a_number_counts_as_sparse() {
        Document index = dcbTagsIndex().append("sparse", 1);

        assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index))).isNull();
    }

    @Test
    void a_direction_that_listIndexes_returns_as_a_double_still_counts_as_ascending() {
        Document index = new Document("v", 2).append("key", new Document("dcbTags", 1.0)).append("name", "dcbTags_1").append("sparse", true);

        assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index))).isNull();
    }

    @Test
    void a_collection_with_no_indexes_is_missing_the_index() {
        assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of())).isEqualTo(DcbTagsIndexCheck.missingIndexMessage(COLLECTION));
    }

    @Test
    void a_compound_index_that_starts_with_dcb_tags_is_not_the_index() {
        Document compound = new Document("v", 2)
                .append("key", new Document("dcbTags", 1).append("position", 1))
                .append("name", "dcbTags_1_position_1")
                .append("sparse", true);

        assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of(compound))).isEqualTo(DcbTagsIndexCheck.missingIndexMessage(COLLECTION));
    }

    @Test
    void a_sparse_descending_index_on_dcb_tags_alone_needs_no_warning() {
        Document descending = new Document("v", 2).append("key", new Document("dcbTags", -1)).append("name", "dcbTags_-1").append("sparse", true);

        assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of(descending))).isNull();
    }

    @Test
    void a_non_sparse_index_with_the_match_all_partial_filter_expression_needs_no_warning() {
        Document index = dcbTagsIndex().append("partialFilterExpression", existsDcbTags());

        assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index))).isNull();
    }

    @Test
    void an_exists_flag_in_the_partial_filter_expression_that_listIndexes_returns_as_a_number_counts_as_true() {
        Document index = dcbTagsIndex().append("partialFilterExpression", new Document("dcbTags", new Document("$exists", 1)));

        assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index))).isNull();
    }

    @Test
    void a_non_sparse_index_on_dcb_tags_alone_is_reported_as_unusable() {
        Document index = dcbTagsIndex();

        String warning = DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index));

        assertAll(
                () -> assertThat(warning).isEqualTo(DcbTagsIndexCheck.unusableIndexMessage(COLLECTION, List.of(index))),
                () -> assertThat(warning).contains("'dcbTags_1'", "isn't sparse", "dropIndex(\"dcbTags_1\")", "createIndex({ dcbTags: 1 }, { sparse: true })")
                        .doesNotContain("collMod")
        );
    }

    @Test
    void a_sparse_index_with_a_partial_filter_expression_is_reported_as_unusable() {
        Document index = dcbTagsIndex().append("sparse", true).append("partialFilterExpression", new Document("type", "Defined"));

        String warning = DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index));

        assertAll(
                () -> assertThat(warning).isEqualTo(DcbTagsIndexCheck.unusableIndexMessage(COLLECTION, List.of(index))),
                () -> assertThat(warning).contains("'dcbTags_1'", "has a partialFilterExpression", "dropIndex(\"dcbTags_1\")")
                        .doesNotContain("isn't sparse", "is hidden", "collMod")
        );
    }

    @Test
    void a_partial_index_that_is_not_sparse_is_reported_for_its_partial_filter_expression_only() {
        Document index = dcbTagsIndex().append("partialFilterExpression", new Document("type", "Defined"));

        String warning = DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index));

        assertAll(
                () -> assertThat(warning).isEqualTo(DcbTagsIndexCheck.unusableIndexMessage(COLLECTION, List.of(index))),
                () -> assertThat(warning).contains("has a partialFilterExpression other than { dcbTags: { $exists: true } }", "dropIndex(\"dcbTags_1\")")
                        .doesNotContain("isn't sparse")
        );
    }

    @Test
    void a_partial_filter_expression_that_only_partly_matches_the_match_all_one_is_reported_as_unusable() {
        Document extraCondition = dcbTagsIndex().append("partialFilterExpression",
                new Document("dcbTags", new Document("$exists", true)).append("type", "Defined"));

        String warning = DcbTagsIndexCheck.warningFor(COLLECTION, List.of(extraCondition));

        assertAll(
                () -> assertThat(warning).isEqualTo(DcbTagsIndexCheck.unusableIndexMessage(COLLECTION, List.of(extraCondition))),
                () -> assertThat(warning).contains("'dcbTags_1'", "has a partialFilterExpression other than { dcbTags: { $exists: true } }", "dropIndex(\"dcbTags_1\")")
                        .doesNotContain("isn't sparse", "collMod")
        );
    }

    @Test
    void a_partial_filter_expression_that_requires_dcb_tags_to_be_missing_is_reported_as_unusable() {
        Document existsFalse = dcbTagsIndex().append("partialFilterExpression", new Document("dcbTags", new Document("$exists", false)));

        String warning = DcbTagsIndexCheck.warningFor(COLLECTION, List.of(existsFalse));

        assertAll(
                () -> assertThat(warning).isEqualTo(DcbTagsIndexCheck.unusableIndexMessage(COLLECTION, List.of(existsFalse))),
                () -> assertThat(warning).contains("'dcbTags_1'", "has a partialFilterExpression other than { dcbTags: { $exists: true } }", "dropIndex(\"dcbTags_1\")")
                        .doesNotContain("isn't sparse", "collMod")
        );
    }

    @Test
    void a_hidden_index_that_is_otherwise_usable_is_fixed_by_unhiding_it_and_not_by_recreating_it() {
        Document index = dcbTagsIndex().append("sparse", true).append("hidden", true);

        String warning = DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index));

        assertAll(
                () -> assertThat(warning).isEqualTo(DcbTagsIndexCheck.unusableIndexMessage(COLLECTION, List.of(index))),
                () -> assertThat(warning).contains("'dcbTags_1'", "is hidden", "collMod: \"events\"", "hidden: false")
                        .doesNotContain("dropIndex", "isn't sparse", "has a partialFilterExpression")
        );
    }

    @Test
    void a_hidden_index_with_the_match_all_partial_filter_expression_is_fixed_by_unhiding_it_and_not_by_recreating_it() {
        Document index = dcbTagsIndex().append("partialFilterExpression", existsDcbTags()).append("hidden", true);

        String warning = DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index));

        assertAll(
                () -> assertThat(warning).isEqualTo(DcbTagsIndexCheck.unusableIndexMessage(COLLECTION, List.of(index))),
                () -> assertThat(warning).contains("'dcbTags_1'", "is hidden", "collMod: \"events\"", "hidden: false")
                        .doesNotContain("dropIndex", "isn't sparse", "partialFilterExpression other than")
        );
    }

    @Test
    void a_hidden_index_that_is_also_not_sparse_is_fixed_by_recreating_it_since_unhiding_alone_would_not_help() {
        Document index = dcbTagsIndex().append("hidden", true);

        String warning = DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index));

        assertAll(
                () -> assertThat(warning).isEqualTo(DcbTagsIndexCheck.unusableIndexMessage(COLLECTION, List.of(index))),
                () -> assertThat(warning).contains("isn't sparse and is hidden", "dropIndex(\"dcbTags_1\")", "createIndex({ dcbTags: 1 }, { sparse: true })")
                        .doesNotContain("collMod")
        );
    }

    @Test
    void a_hidden_flag_set_to_false_does_not_make_the_index_unusable() {
        Document index = dcbTagsIndex().append("sparse", true).append("hidden", false);

        assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index))).isNull();
    }

    @Test
    void a_usable_index_anywhere_in_the_list_wins_over_an_unusable_one() {
        Document unusable = dcbTagsIndex("unusable");
        Document usable = dcbTagsIndex("usable").append("sparse", true);

        assertAll(
                () -> assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of(unusable, usable))).isNull(),
                () -> assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of(usable, unusable))).isNull()
        );
    }

    @Test
    void an_unusable_index_is_reported_even_when_unrelated_indexes_are_present() {
        Document unusable = dcbTagsIndex();

        assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of(positionIndex(), compoundIndex(), unusable)))
                .isEqualTo(DcbTagsIndexCheck.unusableIndexMessage(COLLECTION, List.of(unusable)));
    }

    @Test
    void throws_IllegalArgumentException_when_asked_for_the_unusable_index_message_of_no_indexes() {
        Throwable thrown = catchThrowable(() -> DcbTagsIndexCheck.unusableIndexMessage(COLLECTION, List.of()));

        assertThat(thrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("indexes");
    }

    @Nested
    @DisplayName("when several indexes on dcbTags alone are unusable")
    class When_several_indexes_on_dcb_tags_alone_are_unusable {

        @Test
        void the_warning_names_each_of_them_once_and_no_other_index() {
            Document legacy = dcbTagsIndex("legacy_tags");
            Document partial = dcbTagsIndex("dcbTags_1").append("partialFilterExpression", new Document("type", "x"));

            String warning = DcbTagsIndexCheck.warningFor(COLLECTION, List.of(legacy, compoundIndex(), partial, positionIndex()));

            assertAll(
                    () -> assertThat(warning).containsOnlyOnce("'legacy_tags'"),
                    () -> assertThat(warning).containsOnlyOnce("'dcbTags_1'"),
                    () -> assertThat(warning).doesNotContain("dcbTags_1_position_1", "position_1")
            );
        }

        @Test
        void the_warning_gives_the_reason_of_each_of_them() {
            Document legacy = dcbTagsIndex("legacy_tags");
            Document hidden = dcbTagsIndex("hidden_tags").append("sparse", true).append("hidden", true);

            String warning = DcbTagsIndexCheck.warningFor(COLLECTION, List.of(legacy, hidden));

            assertAll(
                    () -> assertThat(warning).contains("'legacy_tags' isn't sparse."),
                    () -> assertThat(warning).contains("'hidden_tags' is hidden.")
            );
        }

        @Test
        void a_usable_index_after_them_still_means_no_warning() {
            Document legacy = dcbTagsIndex("legacy_tags");
            Document partial = dcbTagsIndex("dcbTags_1").append("partialFilterExpression", new Document("type", "x"));
            Document usable = dcbTagsIndex("usable").append("sparse", true);

            assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of(legacy, partial, usable))).isNull();
        }
    }

    @Nested
    @DisplayName("when the commands in the warning are run in the order they are printed")
    class When_the_commands_in_the_warning_are_run_in_the_order_they_are_printed {

        @Test
        void two_unusable_indexes_are_dropped_before_the_sparse_index_is_created() {
            List<Document> indexes = List.of(
                    dcbTagsIndex("legacy_tags"),
                    dcbTagsIndex("dcbTags_1").append("partialFilterExpression", new Document("type", "x")));

            Outcome outcome = runCommandsOf(DcbTagsIndexCheck.warningFor(COLLECTION, indexes), indexes);

            assertAll(
                    () -> assertThat(outcome.commands()).containsExactly(new DropIndex("legacy_tags"), new DropIndex("dcbTags_1"), new CreateSparseIndex()),
                    () -> assertOneUsableIndexIsLeftIn(outcome.remaining())
            );
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        @DisplayName("a hidden index that holds only DCB events is unhidden and the other unusable index is dropped, whatever order they are listed in")
        void a_hidden_index_that_holds_only_dcb_events_is_unhidden_and_the_other_unusable_index_is_dropped(boolean hiddenFirst) {
            Document hidden = dcbTagsIndex("hidden_tags").append("sparse", true).append("hidden", true);
            Document legacy = dcbTagsIndex("legacy_tags");
            List<Document> indexes = hiddenFirst ? List.of(hidden, legacy) : List.of(legacy, hidden);

            Outcome outcome = runCommandsOf(DcbTagsIndexCheck.warningFor(COLLECTION, indexes), indexes);

            assertAll(
                    () -> assertThat(outcome.commands()).containsExactly(new Unhide("hidden_tags"), new DropIndex("legacy_tags")),
                    () -> assertOneUsableIndexIsLeftIn(outcome.remaining())
            );
        }

        @Test
        void three_unusable_indexes_are_all_dropped_in_the_order_listed_before_the_sparse_index_is_created() {
            List<Document> indexes = List.of(
                    dcbTagsIndex("a"),
                    dcbTagsIndex("dcbTags_1").append("partialFilterExpression", new Document("type", "x")),
                    dcbTagsIndex("c").append("partialFilterExpression", new Document("type", "y")));

            Outcome outcome = runCommandsOf(DcbTagsIndexCheck.warningFor(COLLECTION, indexes), indexes);

            assertAll(
                    () -> assertThat(outcome.commands()).containsExactly(new DropIndex("a"), new DropIndex("dcbTags_1"), new DropIndex("c"), new CreateSparseIndex()),
                    () -> assertOneUsableIndexIsLeftIn(outcome.remaining())
            );
        }

        @Test
        void a_descending_unusable_index_is_dropped_and_the_ascending_sparse_index_is_created() {
            Document descending = new Document("v", 2).append("key", new Document("dcbTags", -1)).append("name", "dcbTags_-1");
            List<Document> indexes = List.of(descending);

            Outcome outcome = runCommandsOf(DcbTagsIndexCheck.warningFor(COLLECTION, indexes), indexes);

            assertAll(
                    () -> assertThat(outcome.commands()).containsExactly(new DropIndex("dcbTags_-1"), new CreateSparseIndex()),
                    () -> assertOneUsableIndexIsLeftIn(outcome.remaining())
            );
        }

        @Test
        void an_index_that_is_only_hidden_gets_the_single_unhide_command_and_no_drop_warning() {
            List<Document> indexes = List.of(dcbTagsIndex("hidden_tags").append("sparse", true).append("hidden", true));

            String warning = DcbTagsIndexCheck.warningFor(COLLECTION, indexes);
            Outcome outcome = runCommandsOf(warning, indexes);

            assertAll(
                    () -> assertThat(outcome.commands()).containsExactly(new Unhide("hidden_tags")),
                    () -> assertThat(warning).doesNotContain("Dropping an index removes it"),
                    () -> assertOneUsableIndexIsLeftIn(outcome.remaining())
            );
        }

        @Test
        void the_commands_that_drop_an_index_end_with_the_sentence_that_says_dropping_removes_it() {
            List<Document> indexes = List.of(dcbTagsIndex("legacy_tags"));

            String warning = DcbTagsIndexCheck.warningFor(COLLECTION, indexes);

            assertThat(warning).endsWith("Dropping an index removes it, so check that nothing else uses it first");
        }

        @Nested
        @TestInstance(TestInstance.Lifecycle.PER_CLASS)
        @DisplayName("and an index name has a character the command has to escape")
        class And_an_index_name_has_a_character_the_command_has_to_escape {

            Stream<Arguments> namesAndTheirPrintedForm() {
                return Stream.of(
                        arguments("we\"ird", "we\\\"ird"),
                        arguments("back\\slash", "back\\\\slash")
                );
            }

            @ParameterizedTest
            @MethodSource("namesAndTheirPrintedForm")
            @DisplayName("the dropIndex command prints the name escaped and dropping it by that name leaves one usable index")
            void the_dropIndex_command_prints_the_name_escaped_and_dropping_it_by_that_name_leaves_one_usable_index(String name, String printed) {
                List<Document> indexes = List.of(dcbTagsIndex(name));

                String warning = DcbTagsIndexCheck.warningFor(COLLECTION, indexes);
                Outcome outcome = runCommandsOf(warning, indexes);

                assertAll(
                        () -> assertThat(warning).contains(".dropIndex(\"" + printed + "\")"),
                        () -> assertThat(outcome.commands()).containsExactly(new DropIndex(name), new CreateSparseIndex()),
                        () -> assertOneUsableIndexIsLeftIn(outcome.remaining())
                );
            }
        }
    }

    @Nested
    @DisplayName("when the command model runs commands in the wrong order")
    class When_the_command_model_runs_commands_in_the_wrong_order {

        @Test
        void throws_AssertionError_when_createIndex_runs_while_dcbTags_1_still_has_other_options() {
            List<Document> indexes = List.of(dcbTagsIndex("dcbTags_1").append("partialFilterExpression", new Document("type", "x")));

            Throwable thrown = catchThrowable(() -> applyInOrder(List.of(new CreateSparseIndex()), indexes));

            assertThat(thrown).isInstanceOf(AssertionError.class).hasMessageContaining("IndexKeySpecsConflict");
        }

        @Test
        void throws_AssertionError_when_dropIndex_names_an_index_that_is_not_there() {
            List<Document> indexes = List.of(dcbTagsIndex("legacy_tags"));

            Throwable thrown = catchThrowable(() -> applyInOrder(List.of(new DropIndex("legacy_tags"), new DropIndex("legacy_tags")), indexes));

            assertThat(thrown).isInstanceOf(AssertionError.class).hasMessageContaining("IndexNotFound");
        }

        @Test
        void throws_AssertionError_when_collMod_names_an_index_that_is_not_there() {
            List<Document> indexes = List.of(dcbTagsIndex("legacy_tags"));

            Throwable thrown = catchThrowable(() -> applyInOrder(List.of(new Unhide("other")), indexes));

            assertThat(thrown).isInstanceOf(AssertionError.class).hasMessageContaining("IndexNotFound");
        }
    }

    @Nested
    @DisplayName("when the partialFilterExpression is an $and")
    class When_the_partial_filter_expression_is_an_and {

        @Test
        void an_and_of_one_copy_of_the_match_all_predicate_needs_no_warning_since_the_planner_on_8_0_34_7_0_45_and_4_2_8_uses_it() {
            Document index = dcbTagsIndex().append("partialFilterExpression", new Document("$and", List.of(existsDcbTags())));

            assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index))).as(PLANNER_NOTE).isNull();
        }

        @Test
        void an_and_of_two_copies_of_the_match_all_predicate_needs_no_warning_since_the_planner_on_8_0_34_7_0_45_and_4_2_8_uses_it() {
            Document index = dcbTagsIndex().append("partialFilterExpression", new Document("$and", List.of(existsDcbTags(), existsDcbTags())));

            assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index))).as(PLANNER_NOTE).isNull();
        }

        @Test
        void a_nested_and_is_reported_as_unusable_since_the_planner_on_4_2_8_ignores_it() {
            Document index = dcbTagsIndex("nested").append("partialFilterExpression",
                    new Document("$and", List.of(new Document("$and", List.of(existsDcbTags())))));

            String warning = DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index));

            assertThat(warning).as(PLANNER_NOTE).contains("'nested'", "has a partialFilterExpression other than");
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        @DisplayName("an and that mixes the predicate with another condition is reported as unusable, whatever order they are in")
        void an_and_that_mixes_the_predicate_with_another_condition_is_reported_as_unusable(boolean predicateFirst) {
            Document other = new Document("type", "x");
            List<Document> operands = predicateFirst ? List.of(existsDcbTags(), other) : List.of(other, existsDcbTags());
            Document index = dcbTagsIndex("mixed").append("partialFilterExpression", new Document("$and", operands));

            String warning = DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index));

            assertThat(warning).contains("'mixed'", "has a partialFilterExpression other than");
        }

        @Test
        void an_and_of_the_predicate_with_exists_false_is_reported_as_unusable() {
            Document index = dcbTagsIndex("missing").append("partialFilterExpression",
                    new Document("$and", List.of(existsDcbTags(), new Document("dcbTags", new Document("$exists", false)))));

            String warning = DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index));

            assertThat(warning).contains("'missing'", "has a partialFilterExpression other than");
        }

        @Test
        void an_and_next_to_another_top_level_condition_is_reported_as_unusable() {
            Document index = dcbTagsIndex("beside").append("partialFilterExpression",
                    new Document("$and", List.of(existsDcbTags())).append("type", "x"));

            String warning = DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index));

            assertThat(warning).contains("'beside'", "has a partialFilterExpression other than");
        }

        @Test
        void an_empty_and_is_reported_as_unusable() {
            Document index = dcbTagsIndex("empty").append("partialFilterExpression", new Document("$and", List.of()));

            String warning = DcbTagsIndexCheck.warningFor(COLLECTION, List.of(index));

            assertThat(warning).contains("'empty'", "has a partialFilterExpression other than");
        }
    }

    private static Document dcbTagsIndex() {
        return dcbTagsIndex("dcbTags_1");
    }

    private static Document dcbTagsIndex(String name) {
        return new Document("v", 2).append("key", new Document("dcbTags", 1)).append("name", name);
    }

    private static Document compoundIndex() {
        return new Document("v", 2).append("key", new Document("dcbTags", 1).append("position", 1)).append("name", "dcbTags_1_position_1").append("sparse", true);
    }

    private static Document positionIndex() {
        return new Document("v", 2).append("key", new Document("position", 1)).append("name", "position_1").append("sparse", true);
    }

    private static Document existsDcbTags() {
        return new Document("dcbTags", new Document("$exists", true));
    }

    // A tiny model of the mongosh commands the warning prints, applied to an in-memory copy of listIndexes.

    private sealed interface Command {
    }

    private record DropIndex(String name) implements Command {
    }

    private record Unhide(String name) implements Command {
    }

    private record CreateSparseIndex() implements Command {
    }

    private record Outcome(List<Command> commands, List<Document> remaining) {
    }

    private static final String QUOTED_NAME = "((?:[^\"\\\\]|\\\\.)*)";
    private static final Pattern COMMAND = Pattern.compile(
            "db\\." + COLLECTION + "\\.dropIndex\\(\"" + QUOTED_NAME + "\"\\)"
                    + "|db\\.runCommand\\(\\{ collMod: \"" + COLLECTION + "\", index: \\{ name: \"" + QUOTED_NAME + "\", hidden: false \\} \\}\\)"
                    + "|db\\." + COLLECTION + "\\.createIndex\\(\\{ dcbTags: 1 \\}, \\{ sparse: true \\}\\)");

    private static Outcome runCommandsOf(String warning, List<Document> indexes) {
        List<Command> commands = commandsIn(warning);
        return new Outcome(commands, applyInOrder(commands, indexes));
    }

    private static List<Command> commandsIn(String warning) {
        List<Command> commands = new ArrayList<>();
        Matcher matcher = COMMAND.matcher(warning);
        while (matcher.find()) {
            if (matcher.group(1) != null) {
                commands.add(new DropIndex(unescaped(matcher.group(1))));
            } else if (matcher.group(2) != null) {
                commands.add(new Unhide(unescaped(matcher.group(2))));
            } else {
                commands.add(new CreateSparseIndex());
            }
        }
        return commands;
    }

    private static String unescaped(String printed) {
        return printed.replace("\\\"", "\"").replace("\\\\", "\\");
    }

    private static List<Document> applyInOrder(List<Command> commands, List<Document> indexes) {
        List<Document> remaining = new ArrayList<>();
        indexes.forEach(index -> remaining.add(new Document(index)));
        for (Command command : commands) {
            if (command instanceof DropIndex drop) {
                if (!remaining.remove(indexNamed(remaining, drop.name(), command))) {
                    throw new AssertionError("IndexNotFound: " + command);
                }
            } else if (command instanceof Unhide unhide) {
                indexNamed(remaining, unhide.name(), command).remove("hidden");
            } else {
                Document created = new Document("v", 2).append("key", new Document("dcbTags", 1)).append("name", "dcbTags_1").append("sparse", true);
                Document existing = remaining.stream().filter(index -> "dcbTags_1".equals(index.getString("name"))).findFirst().orElse(null);
                if (existing == null) {
                    remaining.add(created);
                } else if (!existing.equals(created)) {
                    throw new AssertionError("IndexKeySpecsConflict: an index named dcbTags_1 with other options is still there, " + existing.toJson());
                }
            }
        }
        return remaining;
    }

    private static Document indexNamed(List<Document> indexes, String name, Command command) {
        return indexes.stream().filter(index -> name.equals(index.getString("name"))).findFirst()
                .orElseThrow(() -> new AssertionError("IndexNotFound: " + command));
    }

    private static void assertOneUsableIndexIsLeftIn(List<Document> remaining) {
        List<Document> onDcbTagsAlone = remaining.stream()
                .filter(index -> index.get("key") instanceof Document key && key.size() == 1 && key.containsKey("dcbTags"))
                .toList();
        assertAll(
                () -> assertThat(onDcbTagsAlone).hasSize(1),
                () -> assertThat(DcbTagsIndexCheck.warningFor(COLLECTION, remaining)).isNull()
        );
    }
}
