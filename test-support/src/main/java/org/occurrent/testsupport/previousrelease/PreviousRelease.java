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

package org.occurrent.testsupport.previousrelease;

import org.jspecify.annotations.Nullable;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarFile;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * The jars of an earlier Occurrent release, so a test can run that release's code against what the code under test
 * stored. A {@link Driver} is a Java source compiled against those jars and loaded where every {@code org.occurrent}
 * class comes from them, while every other class, such as the MongoDB driver, Spring and the JDK, is the test's own.
 * A test therefore hands the driver a client or a template it built, and gets back only types from outside
 * {@code org.occurrent}.
 */
public final class PreviousRelease {

    private static final String OCCURRENT_PACKAGE = "org.occurrent.";
    private static final String OCCURRENT_PATH = "org/occurrent/";

    private final List<Path> jars;

    private PreviousRelease(List<Path> jars) {
        this.jars = jars;
    }

    /**
     * The release whose jars are in {@code directory}, which the module's {@code maven-dependency-plugin} execution
     * copies them into before the tests run.
     */
    public static PreviousRelease fromJarsIn(Path directory) {
        requireNonNull(directory, "directory cannot be null");
        List<Path> jars;
        try (Stream<Path> files = Files.list(directory)) {
            jars = files.filter(file -> file.getFileName().toString().endsWith(".jar")).sorted().toList();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot list the jars of the previous release in " + directory.toAbsolutePath()
                    + ". Run the test through Maven, which copies them there in the generate-test-resources phase.", e);
        }
        if (jars.isEmpty()) {
            throw new IllegalStateException("No jars of the previous release in " + directory.toAbsolutePath());
        }
        return new PreviousRelease(jars);
    }

    /**
     * Compiles {@code source}, which declares the public class {@code className} in no package with a
     * {@code public static Object run(Object... args)} method, against this release.
     *
     * @throws IllegalStateException if it does not compile, with what the compiler reported
     */
    public Driver compile(String className, String source) {
        requireNonNull(className, "className cannot be null");
        requireNonNull(source, "source cannot be null");
        JavaCompiler compiler = requireNonNull(ToolProvider.getSystemJavaCompiler(), "The tests must run on a JDK, since a driver is compiled when it is used");
        Path classes;
        try {
            classes = Files.createTempDirectory("occurrent-previous-release");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        List<String> classPath = new ArrayList<>(jars.stream().map(Path::toString).toList());
        classPath.addAll(classPathOutsideOccurrent());
        List<String> options = List.of("-proc:none", "-nowarn", "-classpath", String.join(File.pathSeparator, classPath), "-d", classes.toString());
        StringWriter diagnostics = new StringWriter();
        JavaFileObject file = new SimpleJavaFileObject(URI.create("string:///" + className + ".java"), JavaFileObject.Kind.SOURCE) {
            @Override
            public CharSequence getCharContent(boolean ignoreEncodingErrors) {
                return source;
            }
        };
        if (!compiler.getTask(diagnostics, null, null, options, null, List.of(file)).call()) {
            throw new IllegalStateException("The driver " + className + " does not compile against the previous release:\n" + diagnostics);
        }

        List<URL> urls = new ArrayList<>();
        urls.add(toUrl(classes));
        jars.forEach(jar -> urls.add(toUrl(jar)));
        ClassLoader loader = new URLClassLoader(urls.toArray(URL[]::new), new WithoutOccurrent(PreviousRelease.class.getClassLoader()));
        try {
            Method run = loader.loadClass(className).getMethod("run", Object[].class);
            return new Driver(run);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("The driver " + className + " has no public static Object run(Object... args)", e);
        }
    }

    /**
     * A source compiled against the previous release.
     */
    public static final class Driver {
        private final Method run;

        private Driver(Method run) {
            this.run = run;
        }

        /**
         * Calls the driver's {@code run} with {@code args}, and throws what it throws, unwrapped.
         */
        public @Nullable Object run(Object... args) {
            try {
                return run.invoke(null, (Object) args);
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException runtimeException) {
                    throw runtimeException;
                } else if (cause instanceof Error error) {
                    throw error;
                }
                throw new IllegalStateException(cause);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    // The test's class path without the entries that hold Occurrent classes, so the compiler finds only the previous
    // release's
    private static List<String> classPathOutsideOccurrent() {
        List<String> entries = new ArrayList<>();
        for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
            if (!entry.isEmpty() && !holdsOccurrentClasses(Path.of(entry))) {
                entries.add(entry);
            }
        }
        return entries;
    }

    private static boolean holdsOccurrentClasses(Path entry) {
        if (Files.isDirectory(entry)) {
            return Files.isDirectory(entry.resolve(OCCURRENT_PATH));
        }
        if (!Files.isRegularFile(entry)) {
            return false;
        }
        try (JarFile jar = new JarFile(entry.toFile())) {
            return jar.stream().anyMatch(jarEntry -> jarEntry.getName().startsWith(OCCURRENT_PATH));
        } catch (IOException e) {
            return false;
        }
    }

    private static URL toUrl(Path path) {
        try {
            return path.toUri().toURL();
        } catch (MalformedURLException e) {
            throw new IllegalStateException(e);
        }
    }

    // Hides the test's own Occurrent classes and resources, so a driver's loader finds them only in the previous
    // release's jars
    private static final class WithoutOccurrent extends ClassLoader {
        private WithoutOccurrent(ClassLoader parent) {
            super(parent);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.startsWith(OCCURRENT_PACKAGE)) {
                throw new ClassNotFoundException(name);
            }
            return super.loadClass(name, resolve);
        }

        @Override
        public @Nullable URL getResource(String name) {
            return name.startsWith(OCCURRENT_PATH) ? null : super.getResource(name);
        }

        @Override
        public Enumeration<URL> getResources(String name) throws IOException {
            return name.startsWith(OCCURRENT_PATH) ? Collections.emptyEnumeration() : super.getResources(name);
        }
    }
}
