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

package org.occurrent.tck.eventstore.reactor;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * Finds out whether the compiled classes of a module reference anything capable of skipping a test, which is the only
 * way to check the {@code Assumptions} ban over a whole suite rather than over the lines one fixture happens to reach.
 * See {@link ReactiveSuiteNeverSkipsTest} for why running a suite is not enough on its own.
 * <p>
 * It works on bytes rather than source or reflection. Every type a class mentions is stored in its constant pool as a
 * UTF-8 string in internal form, so {@code org/junit/jupiter/api/Assumptions} appears verbatim in any class that calls
 * it, and comments do not survive compilation, so the javadoc discussing the ban cannot trip the scan. That is worth
 * the byte fiddling: a source scan would have to strip comments to tell a call from a mention of one, and reflection
 * cannot see a method body at all.
 * <p>
 * This is a copy of {@code tck/eventstore-blocking/src/test/java/org/occurrent/tck/eventstore/blocking/SkipMechanismScan.java},
 * for the reason {@code WorkingCheckpointStorage} records for its own copy over in the subscription leaf: a test-sources
 * class in one module is invisible from another, and both ways out cost more than the copy does. Publishing this from
 * {@code tck/common} would add public API that only this build wants, and a {@code test-jar} would add build machinery
 * to share sixty lines.
 */
final class SkipMechanismScan {

    /**
     * Every way a JUnit test can end up reported as skipped or aborted rather than passed or failed. Internal names,
     * because that is the form a constant pool holds, and prefixes are allowed: the whole
     * {@code org.junit.jupiter.api.condition} package is {@code @EnabledIf}/{@code @DisabledIf} annotations, every one
     * of which skips.
     */
    private static final List<String> WAYS_TO_SKIP = List.of(
            "org/junit/jupiter/api/Assumptions",              // assumeTrue, assumeFalse, assumingThat
            "org/assertj/core/api/Assumptions",               // AssertJ's own assumeThat
            "org/junit/Assume",                               // JUnit 4, in case it ever reaches the classpath
            "org/opentest4j/TestAbortedException",            // thrown by hand, which is what an assumption does
            "org/opentest4j/IncompleteExecutionException",    // its supertype, and abortive on its own
            "org/junit/jupiter/api/Disabled",                 // a test that never runs reports as skipped
            "org/junit/jupiter/api/condition/"                // @EnabledOnOs, @DisabledIfSystemProperty, and the rest
    );

    private SkipMechanismScan() {
    }

    /**
     * Scans every class compiled alongside {@code anchor} and reports which of them reference a way to skip.
     *
     * @return offending class names to the references found in them, in name order, empty when nothing was found
     */
    static SortedMap<String, List<String>> of(Class<?> anchor) {
        Path root = outputDirectoryOf(anchor);
        SortedMap<String, List<String>> offenders = new TreeMap<>();
        try (Stream<Path> classFiles = Files.walk(root)) {
            classFiles.filter(path -> path.getFileName().toString().endsWith(".class")).forEach(classFile -> {
                List<String> found = waysToSkipReferencedBy(classFile);
                if (!found.isEmpty()) {
                    offenders.put(classNameOf(root, classFile), found);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException("Could not walk " + root, e);
        }
        return offenders;
    }

    /**
     * The classes {@link #of(Class)} looked at, so that a scan finding nothing can be told apart from a scan that never
     * looked at anything.
     */
    static List<String> classesScannedAlongside(Class<?> anchor) {
        Path root = outputDirectoryOf(anchor);
        try (Stream<Path> classFiles = Files.walk(root)) {
            return classFiles.filter(path -> path.getFileName().toString().endsWith(".class"))
                    .map(classFile -> classNameOf(root, classFile))
                    .sorted()
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("Could not walk " + root, e);
        }
    }

    private static Path outputDirectoryOf(Class<?> anchor) {
        CodeSource codeSource = anchor.getProtectionDomain().getCodeSource();
        if (codeSource == null) {
            throw new IllegalStateException(anchor.getName() + " has no code source, so there is nothing to scan");
        }
        Path location;
        try {
            location = Path.of(codeSource.getLocation().toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Cannot read the code source of " + anchor.getName(), e);
        }
        if (!Files.isDirectory(location)) {
            throw new IllegalStateException(anchor.getName() + " was loaded from " + location
                    + " rather than from a directory of compiled classes, so the anchor belongs to another module and "
                    + "the scan would silently cover the wrong classes");
        }
        return location;
    }

    private static List<String> waysToSkipReferencedBy(Path classFile) {
        byte[] bytecode;
        try {
            bytecode = Files.readAllBytes(classFile);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not read " + classFile, e);
        }
        List<String> found = new ArrayList<>();
        for (String wayToSkip : WAYS_TO_SKIP) {
            if (contains(bytecode, wayToSkip.getBytes(StandardCharsets.UTF_8))) {
                found.add(wayToSkip);
            }
        }
        return found;
    }

    private static boolean contains(byte[] haystack, byte[] needle) {
        outer:
        for (int start = 0; start <= haystack.length - needle.length; start++) {
            for (int offset = 0; offset < needle.length; offset++) {
                if (haystack[start + offset] != needle[offset]) {
                    continue outer;
                }
            }
            return true;
        }
        return false;
    }

    private static String classNameOf(Path root, Path classFile) {
        String relative = root.relativize(classFile).toString();
        return relative.substring(0, relative.length() - ".class".length())
                .replace(File.separatorChar, '.')
                .replace('/', '.');
    }
}
