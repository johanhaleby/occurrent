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
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every message that links {@link SharedStorageMessages#GUIDE} sends the reader to a heading by its slug, which the
 * compiler cannot check, so a reworded heading would leave every link landing at the top of the page.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class GuideAnchorTest {

    private static final String BLOB_OF_MAIN = "/blob/main/";
    private static final Pattern HEADING = Pattern.compile("^#{1,6}\\s+(.+?)\\s*$");
    private static final Pattern NOT_PART_OF_A_SLUG = Pattern.compile("[^\\p{L}\\p{N} _-]");

    @Test
    void the_guide_link_ends_in_the_slug_of_a_heading_of_the_page_it_names() throws IOException {
        String guide = SharedStorageMessages.GUIDE;
        String anchor = guide.substring(guide.indexOf('#') + 1);
        String page = guide.substring(guide.indexOf(BLOB_OF_MAIN) + BLOB_OF_MAIN.length(), guide.indexOf('#'));

        List<String> slugs = slugsOfTheHeadingsIn(NewIdStepCopiesTest.repositoryRoot().resolve(page));

        assertThat(slugs).as("the slugs of the headings in " + page + ", which has to hold the anchor \"" + anchor + "\" that SharedStorageMessages.GUIDE links to").contains(anchor);
    }

    @Test
    void a_slug_is_the_lower_case_heading_without_punctuation_or_backticks_and_with_hyphens_for_spaces() {
        assertThat(slugOf("Changing a `checkpoint`, or marker: that several_instances share-it?"))
                .isEqualTo("changing-a-checkpoint-or-marker-that-several_instances-share-it");
    }

    @Test
    void a_line_that_starts_with_a_hash_inside_a_fenced_code_block_is_not_a_heading() throws IOException {
        Path page = Files.createTempFile("guide-anchor", ".md");
        try {
            Files.writeString(page, "# Real\n```\n# not a heading\n```\n## Also real\n");

            assertThat(slugsOfTheHeadingsIn(page)).containsExactly("real", "also-real");
        } finally {
            Files.delete(page);
        }
    }

    private static List<String> slugsOfTheHeadingsIn(Path page) throws IOException {
        List<String> slugs = new ArrayList<>();
        boolean inFence = false;
        for (String line : Files.readAllLines(page)) {
            if (line.stripLeading().startsWith("```")) {
                inFence = !inFence;
            } else if (!inFence) {
                var heading = HEADING.matcher(line);
                if (heading.matches()) {
                    slugs.add(slugOf(heading.group(1)));
                }
            }
        }
        return slugs;
    }

    private static String slugOf(String heading) {
        return NOT_PART_OF_A_SLUG.matcher(heading.toLowerCase(Locale.ROOT)).replaceAll("").replace(' ', '-');
    }
}
