/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.occurrent.subscription.internal;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link SharedStorageMessages#NEW_ID_WHEN_SEVERAL_INSTANCES_RUN_IT} is copied by hand into javadoc, which cannot
 * reference a constant. This keeps every copy saying what the constant says.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class NewIdStepCopiesTest {

    private static final String START_OF_A_COPY = "Under a new id the subscription starts from the beginning of the history";
    private static final String END_OF_A_COPY = "describes.";
    private static final String START_OF_THE_SHARED_TAIL = "Stop every instance on the old id";
    private static final String GUIDE_LINK = "<a href=\"" + SharedStorageMessages.GUIDE + "\">the upgrade guide</a>";
    private static final Pattern WHITESPACE = Pattern.compile("\\s+");
    private static final Pattern LEADING_WHITESPACE_AND_STAR = Pattern.compile("^\\s*\\*?\\s*");

    // Every file that holds a copy, and how many, so that a copy that is added or removed in one of these files is a decision and not a drift
    private static final Map<String, Integer> COPIES = copies();

    private static Map<String, Integer> copies() {
        Map<String, Integer> copies = new LinkedHashMap<>();
        copies.put("dsl/projection-dsl/blocking/src/main/java/org/occurrent/dsl/projection/blocking/CatchupProjectionFeed.java", 1);
        copies.put("dsl/projection-dsl/blocking/src/main/java/org/occurrent/dsl/projection/blocking/DomainEventFeed.java", 1);
        copies.put("dsl/projection-dsl/reactor/src/main/java/org/occurrent/dsl/projection/reactor/CatchupProjectionFeed.java", 1);
        copies.put("dsl/projection-dsl/reactor/src/main/java/org/occurrent/dsl/projection/reactor/DomainEventFeed.java", 1);
        copies.put("framework/spring-boot-autoconfigure/common/src/main/java/org/occurrent/springboot/common/PushCatchupStatus.java", 1);
        copies.put("subscription/api/blocking/src/main/java/org/occurrent/subscription/api/blocking/CancellableSubscriptions.java", 1);
        copies.put("subscription/api/reactor/src/main/java/org/occurrent/subscription/api/reactor/CancellableSubscriptions.java", 1);
        copies.put("subscription/core/src/main/java/org/occurrent/subscription/RoutingOutcome.java", 1);
        copies.put("subscription/push/blocking/src/main/java/org/occurrent/subscription/push/blocking/CatchupThenPushSubscriptionModel.java", 1);
        copies.put("subscription/push/reactor/src/main/java/org/occurrent/subscription/push/reactor/CatchupThenPushSubscriptionModel.java", 3);
        return copies;
    }

    @Test
    void every_copy_of_the_step_for_a_new_id_in_a_javadoc_says_what_the_constant_says() throws IOException {
        Path root = repositoryRoot();

        List<String> problems = new ArrayList<>();
        for (Map.Entry<String, Integer> file : COPIES.entrySet()) {
            Path path = root.resolve(file.getKey());
            if (!Files.isRegularFile(path)) {
                problems.add(file.getKey() + " is missing");
                continue;
            }
            List<String> found = copiesIn(path);
            if (found.size() != file.getValue()) {
                problems.add(file.getKey() + " holds " + found.size() + " copies, expected " + file.getValue());
            }
            for (String copy : found) {
                if (!copy.equals(SharedStorageMessages.NEW_ID_WHEN_SEVERAL_INSTANCES_RUN_IT)) {
                    problems.add(file.getKey() + " holds a copy that differs from the constant:\n  copy:     " + copy + "\n  constant: " + SharedStorageMessages.NEW_ID_WHEN_SEVERAL_INSTANCES_RUN_IT);
                }
            }
        }

        assertThat(problems).as("what differs between the constant and its copies").isEmpty();
    }

    // Saga words the start of the step for a saga, so only the tail it shares with the constant is compared
    @Test
    void the_javadoc_of_saga_holds_once_the_tail_of_the_step_for_a_new_id_that_it_shares_with_the_constant() throws IOException {
        String constant = SharedStorageMessages.NEW_ID_WHEN_SEVERAL_INSTANCES_RUN_IT;
        String tail = constant.substring(constant.indexOf(START_OF_THE_SHARED_TAIL));
        String saga = "framework/annotations/src/main/java/org/occurrent/annotation/Saga.java";

        String text = normalise(Files.readString(repositoryRoot().resolve(saga))).replace(GUIDE_LINK, SharedStorageMessages.GUIDE);

        assertThat(text).as("the text of " + saga + " with its lines joined").containsOnlyOnce(tail);
    }

    @Test
    void the_constant_is_found_in_the_normalised_text_of_this_files_own_source_so_that_the_search_for_a_copy_is_known_to_work() throws IOException {
        Path source = repositoryRoot().resolve("subscription/core/src/main/java/org/occurrent/subscription/internal/SharedStorageMessages.java");

        // The constant is concatenated from quoted parts there, so only its start is a copy of the kind searched for
        assertThat(normalise(Files.readString(source))).contains(START_OF_A_COPY);
    }

    private static List<String> copiesIn(Path path) throws IOException {
        String text = normalise(Files.readString(path));
        List<String> copies = new ArrayList<>();
        int start = text.indexOf(START_OF_A_COPY);
        while (start >= 0) {
            int end = text.indexOf(END_OF_A_COPY, start);
            if (end < 0) {
                copies.add(text.substring(start));
                break;
            }
            end += END_OF_A_COPY.length();
            copies.add(text.substring(start, end).replace(GUIDE_LINK, SharedStorageMessages.GUIDE));
            start = text.indexOf(START_OF_A_COPY, end);
        }
        return copies;
    }

    // Drops each line's leading whitespace and star and joins the lines, so that a copy compares the same wherever its lines break
    private static String normalise(String source) {
        StringBuilder joined = new StringBuilder();
        for (String line : source.split("\\R")) {
            joined.append(LEADING_WHITESPACE_AND_STAR.matcher(line).replaceFirst("")).append(' ');
        }
        return WHITESPACE.matcher(joined).replaceAll(" ").trim();
    }

    // Surefire runs in the module directory, so walk up to the directory that holds the modules
    static Path repositoryRoot() {
        Path directory = Path.of("").toAbsolutePath();
        for (Path candidate = directory; candidate != null; candidate = candidate.getParent()) {
            if (Files.isRegularFile(candidate.resolve("AGENTS.md")) && Files.isRegularFile(candidate.resolve("subscription/core/pom.xml"))) {
                return candidate;
            }
        }
        throw new IllegalStateException("Cannot find the repository root from " + directory + ", which is the directory the tests run in. It is the nearest directory above it that holds both AGENTS.md and subscription/core/pom.xml.");
    }
}
