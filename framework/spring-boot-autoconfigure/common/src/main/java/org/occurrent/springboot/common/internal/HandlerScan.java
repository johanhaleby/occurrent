/*
 *
 *  Copyright 2024 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.springboot.common.internal;

import org.jspecify.annotations.Nullable;
import org.occurrent.annotation.StreamId;
import org.occurrent.annotation.StreamVersion;
import org.occurrent.springboot.common.SubscriptionAnnotations;
import org.occurrent.springboot.common.SubscriptionAnnotations.HandlerParameter;
import org.occurrent.springboot.common.SubscriptionAnnotations.HandlerParameterKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.scope.ScopedProxyUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.core.BridgeMethodResolver;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.core.annotation.MergedAnnotations.SearchStrategy;
import org.springframework.util.ClassUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * How the Spring Boot starters find a bean's handler methods, read their types against the bean's class, and decide which method a call runs on a proxy. Internal to the Spring Boot starters, not an API, so the scan rules can change in a minor release.
 */
public final class HandlerScan {

    private static final Logger log = LoggerFactory.getLogger(HandlerScan.class);

    private HandlerScan() {
    }

    /**
     * Classify each parameter of a subscription handler as {@link SubscriptionAnnotations#analyzeParameters(Method, Predicate, boolean)}
     * does, reading each parameter's type as {@code implementation} sees it. A handler that {@code implementation}
     * inherits from a generic superclass or interface, such as {@code on(E event)} in a {@code Base<E>} that
     * {@code implementation} extends as {@code Base<OrderPlaced>}, then has {@code OrderPlaced} as its event type. A type
     * variable that {@code implementation} does not bind, through a raw supertype for example, keeps the type the
     * method declares, its bound or {@code Object}. A bridge method's types are read from the method it bridges.
     *
     * @param method                  the handler method
     * @param implementation          the class whose instances the handler runs on, see {@link #implementationClassOf(Object)}
     * @param isMetadataParameter     whether a parameter type is a metadata parameter
     * @param supportsStreamAccessors whether {@code @StreamId}/{@code @StreamVersion} parameters are allowed
     * @return the classified parameters, in declaration order
     */
    public static List<HandlerParameter> analyzeParameters(Method method, Class<?> implementation, Predicate<Class<?>> isMetadataParameter, boolean supportsStreamAccessors) {
        // The bridge javac adds to a public class for a public method it inherits from a superclass that isn't public
        // has the erased parameter types and no generic signature, so the types are read from the method it bridges
        return classifyParameters(method, BridgeMethodResolver.findBridgedMethod(method), implementation, isMetadataParameter, supportsStreamAccessors);
    }

    /**
     * Classify each parameter of a subscription handler, reading each parameter's type from {@code method} itself as
     * its declaring class sees it, so a type variable gives its bound and a bridge method its erased types, as
     * {@link SubscriptionAnnotations#analyzeParameters(Method, Predicate, boolean)} did in 0.34.0.
     *
     * @param method                  the handler method
     * @param isMetadataParameter     whether a parameter type is a metadata parameter
     * @param supportsStreamAccessors whether {@code @StreamId}/{@code @StreamVersion} parameters are allowed
     * @return the classified parameters, in declaration order
     */
    public static List<HandlerParameter> analyzeDeclaredParameters(Method method, Predicate<Class<?>> isMetadataParameter, boolean supportsStreamAccessors) {
        return classifyParameters(method, method, method.getDeclaringClass(), isMetadataParameter, supportsStreamAccessors);
    }

    private static List<HandlerParameter> classifyParameters(Method method, Method typeSource, Class<?> implementation, Predicate<Class<?>> isMetadataParameter, boolean supportsStreamAccessors) {
        List<HandlerParameter> parameters = new ArrayList<>();
        boolean hasEvent = false;
        boolean hasMetadata = false;
        boolean hasStreamId = false;
        boolean hasStreamVersion = false;
        Parameter[] declaredParameters = method.getParameters();
        for (int index = 0; index < declaredParameters.length; index++) {
            Parameter parameter = declaredParameters[index];
            Class<?> type = ResolvableType.forMethodParameter(typeSource, index, implementation).resolve(typeSource.getParameterTypes()[index]);
            boolean streamIdAnnotated = parameter.isAnnotationPresent(StreamId.class);
            boolean streamVersionAnnotated = parameter.isAnnotationPresent(StreamVersion.class);
            if (streamIdAnnotated && streamVersionAnnotated) {
                throw new IllegalArgumentException("A subscription parameter may not be annotated with both @StreamId and @StreamVersion, but %s#%s declares one that is.".formatted(method.getDeclaringClass().getName(), method.getName()));
            }
            HandlerParameterKind kind;
            if (streamIdAnnotated) {
                if (!supportsStreamAccessors) {
                    throw new IllegalArgumentException("@StreamId is only supported on @Subscription, @StreamSubscription, and @SynchronousSubscription handlers, but %s#%s declares it.".formatted(method.getDeclaringClass().getName(), method.getName()));
                }
                if (type != String.class) {
                    throw new IllegalArgumentException("A @StreamId parameter must be of type String, but %s#%s declares it as %s.".formatted(method.getDeclaringClass().getName(), method.getName(), type.getName()));
                }
                if (hasStreamId) {
                    throw new IllegalArgumentException("A subscription method may declare at most one @StreamId parameter, but %s#%s declares more than one.".formatted(method.getDeclaringClass().getName(), method.getName()));
                }
                hasStreamId = true;
                kind = HandlerParameterKind.STREAM_ID;
            } else if (streamVersionAnnotated) {
                if (!supportsStreamAccessors) {
                    throw new IllegalArgumentException("@StreamVersion is only supported on @Subscription, @StreamSubscription, and @SynchronousSubscription handlers, but %s#%s declares it.".formatted(method.getDeclaringClass().getName(), method.getName()));
                }
                if (type != long.class && type != Long.class) {
                    throw new IllegalArgumentException("A @StreamVersion parameter must be of type long or Long, but %s#%s declares it as %s.".formatted(method.getDeclaringClass().getName(), method.getName(), type.getName()));
                }
                if (hasStreamVersion) {
                    throw new IllegalArgumentException("A subscription method may declare at most one @StreamVersion parameter, but %s#%s declares more than one.".formatted(method.getDeclaringClass().getName(), method.getName()));
                }
                hasStreamVersion = true;
                kind = HandlerParameterKind.STREAM_VERSION;
            } else if (isMetadataParameter.test(type)) {
                if (hasMetadata) {
                    throw new IllegalArgumentException("A subscription method may declare at most one metadata parameter, but %s#%s declares more than one.".formatted(method.getDeclaringClass().getName(), method.getName()));
                }
                hasMetadata = true;
                kind = HandlerParameterKind.METADATA;
            } else {
                if (hasEvent) {
                    throw new IllegalArgumentException("A subscription method may declare only one event parameter, but %s#%s declares more than one.".formatted(method.getDeclaringClass().getName(), method.getName()));
                }
                hasEvent = true;
                kind = HandlerParameterKind.EVENT;
            }
            parameters.add(new HandlerParameter(type, kind));
        }
        return parameters;
    }

    /**
     * The class whose code runs when a handler or factory is invoked on {@code bean}, which is the user class,
     * {@link ClassUtils#getUserClass}, of the object {@link SubscriptionAnnotations#ultimateTarget} unwraps
     * {@code bean} to. A proxy whose target source is not a fixed singleton is not unwrapped, so a CGLIB proxy like
     * that answers with the class it subclasses, and a JDK interface proxy like that with its own proxy class, as in
     * 0.34.0. Both annotation post-processors scan this class, and handler and factory generics are read against it,
     * so the two agree on one {@link Method} per handler.
     *
     * @param bean the (possibly proxied) bean
     * @return the class a bean is scanned for handlers in, and a handler's or factory's generic types are read against
     */
    public static Class<?> implementationClassOf(Object bean) {
        // The target of a JDK proxy that isn't unwrapped can be a bean of its own, scanned under its own name, so the
        // proxy class is read instead of the class its target source names, or the handlers would register twice
        return ClassUtils.getUserClass(SubscriptionAnnotations.ultimateTarget(bean).getClass());
    }

    /**
     * The method to invoke on {@code bean} to run {@code method}, the one {@link AopUtils#selectInvocableMethod} selects
     * for {@code bean}'s class. A JDK interface proxy implements only its interfaces, and the scan reads the class
     * behind it, so when the scanned method overrides a generic interface method, such as {@code on(Placed)} for
     * {@code on(E)}, no interface declares the scanned signature and the selection fails. The interface method that
     * the class behind the proxy implements with {@code method} is returned instead, so the call still goes through
     * the proxy and its advice. When the selection is an interface method of the same signature that {@code method}
     * doesn't implement, such as a default method beside a private handler of a superclass, invoking it would run
     * another body under the handler's id, so it is refused.
     *
     * @param bean   the (possibly proxied) object the method is about to be invoked on
     * @param method the method the scan read
     * @return the method to invoke on {@code bean}
     * @throws IllegalStateException when {@code bean} has no method that runs {@code method}
     */
    public static Method invocableMethodOn(Object bean, Method method) {
        Method selected;
        try {
            selected = AopUtils.selectInvocableMethod(method, bean.getClass());
        } catch (IllegalStateException e) {
            Method interfaceMethod = proxyInterfaceMethodRunning(bean, method);
            if (interfaceMethod == null) {
                throw e;
            }
            return interfaceMethod;
        }
        if (!selected.equals(method) && !selectionRuns(bean, selected, method)) {
            throw new IllegalStateException("Invoking %s on %s runs %s instead".formatted(method, bean.getClass().getName(), selected));
        }
        return selected;
    }

    // selectInvocableMethod falls back to a method of the same signature on the proxy's interfaces, which runs method
    // only when method implements it. An interface method is public, so a private or package-private method never
    // does. The class behind a proxy that isn't unwrapped can't be read, so there only the modifiers decide
    private static boolean selectionRuns(Object bean, Method selected, Method method) {
        if (!Modifier.isPublic(method.getModifiers())) {
            return false;
        }
        Class<?> implementation = implementationClassOf(bean);
        return java.lang.reflect.Proxy.isProxyClass(implementation) || AopUtils.getMostSpecificMethod(selected, implementation).equals(method);
    }

    private static @Nullable Method proxyInterfaceMethodRunning(Object bean, Method method) {
        if (!java.lang.reflect.Proxy.isProxyClass(bean.getClass())) {
            return null;
        }
        Class<?> implementation = implementationClassOf(bean);
        for (Class<?> proxiedInterface : bean.getClass().getInterfaces()) {
            for (Method interfaceMethod : proxiedInterface.getMethods()) {
                if (!Modifier.isStatic(interfaceMethod.getModifiers()) && interfaceMethod.getName().equals(method.getName())
                        && interfaceMethod.getParameterCount() == method.getParameterCount()
                        && AopUtils.getMostSpecificMethod(interfaceMethod, implementation).equals(method)) {
                    return interfaceMethod;
                }
            }
        }
        return null;
    }

    /**
     * Answers whether {@code method} is package-private and declared in another runtime package than
     * {@code implementation}, which is another package or the same package name in another class loader. A CGLIB proxy
     * of {@code implementation} is generated in {@code implementation}'s runtime package, so it cannot override
     * such a method, and invoking the method on the proxy runs the method's own body on the proxy instance, with no
     * advice and none of the bean's state.
     *
     * @param method         the handler or factory method
     * @param implementation the class a CGLIB proxy in the bean's chain subclasses
     * @return {@code true} if a CGLIB proxy of {@code implementation} cannot override {@code method} for being package-private
     */
    public static boolean isPackagePrivateOutsidePackageOf(Method method, Class<?> implementation) {
        int modifiers = method.getModifiers();
        return !Modifier.isPublic(modifiers) && !Modifier.isProtected(modifiers) && !Modifier.isPrivate(modifiers)
                && !inSameRuntimePackage(method.getDeclaringClass(), implementation);
    }

    /**
     * Read type argument {@code index} of {@code method}'s return type against {@code implementation} and return its
     * class. A factory that {@code implementation} inherits from a generic superclass or interface, such as
     * {@code Projection<S, MyEvent, String> projection()} in a {@code Base<S>} that {@code implementation} extends as
     * {@code Base<MyView>}, then names {@code MyView}. A parameterized argument names its raw class.
     *
     * @param method         the factory method
     * @param implementation the class whose instance the factory runs on, see {@link #implementationClassOf(Object)}
     * @param index          the type argument to read
     * @return the class, or {@code null} when the return type has no such type argument, or the argument is a wildcard
     * or a type variable that {@code implementation} does not bind, through a raw supertype or a type parameter of its
     * own for example, or one the method itself declares
     */
    public static @Nullable Class<?> returnTypeArgument(Method method, Class<?> implementation, int index) {
        // A bridge has the raw return type, so it is read from the method the bridge calls, as analyzeParameters does
        if (!(BridgeMethodResolver.findBridgedMethod(method).getGenericReturnType() instanceof ParameterizedType returnType) || returnType.getActualTypeArguments().length <= index) {
            return null;
        }
        Type argument = returnType.getActualTypeArguments()[index];
        // ResolvableType.resolve() falls back to a variable's bound, which names no class the factory chose, so the
        // binding is followed by hand and an unbound variable answers null
        while (argument instanceof TypeVariable<?> variable) {
            if (!(variable.getGenericDeclaration() instanceof Class<?> declaringClass)) {
                return null;
            }
            int position = Arrays.asList(declaringClass.getTypeParameters()).indexOf(variable);
            Type binding = ResolvableType.forClass(implementation).as(declaringClass).getGeneric(position).getType();
            if (binding.equals(variable)) {
                return null;
            }
            argument = binding;
        }
        if (argument instanceof Class<?> type) {
            return type;
        }
        if (argument instanceof ParameterizedType parameterized && parameterized.getRawType() instanceof Class<?> raw) {
            return raw;
        }
        return null;
    }

    /**
     * The methods of {@code type} annotated with any of {@code annotationTypes}, read from {@code type}, its
     * superclasses and its interfaces, so an inherited handler and an interface default method count the way a
     * declared one does.
     * Each method is the one {@code type} actually runs, so an override is found once, under its own annotation if it
     * repeats one and under the overridden method's if it doesn't. Generic bridge methods are left out, so a generic
     * handler is found once too. A public class that inherits a public method from a package-private superclass gives
     * the bridge method javac generates for it, which calls the inherited method. Overriding follows the JVM's rules, under which a package-private method is overridden only
     * from its own package or through a chain of overrides that starts there. Spring's annotation search ignores
     * packages, so it can give a method the annotation of a package-private method the method doesn't override. Such a
     * method is refused with an {@link IllegalStateException}, since nothing says which of the two bodies the handler
     * is, unless {@code type} or a superclass below it declares a method of the same signature, which is then checked
     * instead. A package-private method that a method of {@code type} or one of its superclasses overrides is left out, since
     * {@code type} never runs it. A static method an interface declares is left out, since a class does not inherit
     * it. A static method a superclass declares is returned like any other, so a static subscription handler there is
     * refused the way one the class declares is. Both annotation post-processors call this, so they find the same
     * methods.
     * <p>
     * A package-private annotated method is returned when {@code type} runs it, even when a method of the same
     * signature from another package sits between it and {@code type}. That method doesn't override it, and is
     * returned on its own when it has a handler annotation. When that method is public or protected, a CGLIB proxy of
     * {@code type} runs it for both, which {@link #methodACglibProxyRunsInsteadOf} answers so the caller can refuse
     * the package-private one.
     *
     * @param type            the concrete class the scan reads
     * @param annotationTypes the handler annotations to look for, searched with {@link SearchStrategy#TYPE_HIERARCHY}
     * @return the matching methods, empty if there are none. When a method of a superclass or interface of
     * {@code type} names a class missing at runtime, only the methods {@code type} declares are matched, which is
     * logged at debug level
     * @throws NoClassDefFoundError  when a method {@code type} declares names a class missing at runtime, as in 0.34.0
     * @throws IllegalStateException when a method has an annotation Spring reads from a method it does not override
     * @throws IllegalArgumentException when {@code type} is a JDK proxy and two of its interfaces declare a method of
     *                                  the same name and parameter types with different handler annotations
     */
    public static Set<Method> handlerMethods(Class<?> type, List<Class<? extends Annotation>> annotationTypes) {
        if (!AnnotationUtils.isCandidateClass(type, annotationTypes)) {
            return Set.of();
        }
        MethodIntrospector.MetadataLookup<Boolean> handlerLookup = method -> {
            if (Modifier.isStatic(method.getModifiers()) && method.getDeclaringClass().isInterface()) {
                return null;
            }
            MergedAnnotations annotations = MergedAnnotations.from(method, SearchStrategy.TYPE_HIERARCHY);
            // MethodIntrospector can hand over a bridge method and keeps the method it bridges instead, so that is the
            // method the checks below compare against
            Method bridged = BridgeMethodResolver.findBridgedMethod(method);
            boolean annotated = false;
            for (Class<? extends Annotation> annotationType : annotationTypes) {
                MergedAnnotation<? extends Annotation> annotation = annotations.get(annotationType);
                if (!annotation.isPresent()) {
                    continue;
                }
                // MethodIntrospector keeps a package-private method of another package than type even when a method of
                // its own package overrides it, since Spring decides overriding by type's package alone
                if (!methodTheJvmRuns(bridged, type).equals(bridged)) {
                    return null;
                }
                if (annotation.getSource() instanceof Method declaration && !methodTheJvmRuns(declaration, type).equals(bridged)) {
                    // A method of the same signature declared below it is the one the class shows for that
                    // signature, and it is checked on its own, as a handler or refused
                    if (declaredBelow(bridged, type)) {
                        return null;
                    }
                    throw annotationFromAMethodNotOverridden(bridged, declaration, annotation);
                }
                annotated = true;
            }
            return annotated ? Boolean.TRUE : null;
        };
        // A method type itself declares that names a class missing at runtime fails the scan, as in 0.34.0
        type.getDeclaredMethods();
        Set<Method> handlers;
        try {
            handlers = new LinkedHashSet<>(MethodIntrospector.selectMethods(type, handlerLookup).keySet());
            // MethodIntrospector reads a package-private method through ClassUtils.getMostSpecificMethod, which takes
            // the first method of the same signature up from type when type is in the method's package, even a method
            // of another package that doesn't override it. The method MethodIntrospector never inspects is still the
            // one the JVM runs, so each package-private method that ClassUtils.getMostSpecificMethod replaces is
            // inspected here
            for (Class<?> candidateClass = type; candidateClass != null && candidateClass != Object.class; candidateClass = candidateClass.getSuperclass()) {
                for (Method method : candidateClass.getDeclaredMethods()) {
                    int modifiers = method.getModifiers();
                    boolean packagePrivate = !Modifier.isPublic(modifiers) && !Modifier.isProtected(modifiers) && !Modifier.isPrivate(modifiers);
                    if (packagePrivate && !method.isSynthetic() && ClassUtils.getMostSpecificMethod(method, type) != method
                            && !handlers.contains(method) && Boolean.TRUE.equals(handlerLookup.inspect(method))) {
                        handlers.add(method);
                    }
                }
            }
        } catch (IllegalStateException | LinkageError e) {
            if (!isReadFailure(e)) {
                throw e;
            }
            // When a superclass or interface method names a class missing at runtime, the scan reads only the methods
            // type declares itself, as 0.34.0 did
            log.debug("Reading only the methods {} declares in the search for Occurrent handler annotations, since its superclasses and interfaces can't be read", type.getName(), e);
            handlers = declaredHandlerMethods(type, handlerLookup);
        }
        return java.lang.reflect.Proxy.isProxyClass(type) ? firstOfEachSignature(type, handlers, annotationTypes) : handlers;
    }

    // ReflectionUtils reports a class it can't read as an IllegalStateException caused by the LinkageError, and a
    // refusal of the handler lookup has no cause
    private static boolean isReadFailure(Throwable e) {
        return e instanceof LinkageError || e.getCause() instanceof LinkageError;
    }

    // The handlers among the methods type declares, for a type whose superclasses or interfaces can't be read. A bridge
    // counts as the method it calls, as MethodIntrospector counts it. A method is left out when finding the method a
    // bridge calls, or checking a method for a handler annotation, has to read a class missing at runtime
    private static Set<Method> declaredHandlerMethods(Class<?> type, MethodIntrospector.MetadataLookup<Boolean> handlerLookup) {
        Set<Method> handlers = new LinkedHashSet<>();
        for (Method method : type.getDeclaredMethods()) {
            if (method.isSynthetic() && !method.isBridge()) {
                continue;
            }
            try {
                Method candidate = BridgeMethodResolver.findBridgedMethod(method);
                if (Boolean.TRUE.equals(handlerLookup.inspect(candidate))) {
                    handlers.add(candidate);
                }
            } catch (IllegalStateException | LinkageError e) {
                if (!isReadFailure(e)) {
                    throw e;
                }
                log.debug("Skipping {} in the search for Occurrent handler annotations, since a class it depends on can't be read", method, e);
            }
        }
        return handlers;
    }

    /**
     * Answers which method a CGLIB proxy of {@code implementation} runs when {@code method} is invoked on it, when that
     * is another method than {@code method}. A CGLIB proxy overrides one method for each name and parameter types, the
     * first instance method up from {@code implementation} that is public, protected, or package-private in
     * {@code implementation}'s package. When {@code method} is package-private and a public or protected method of the
     * same signature from another package sits between {@code implementation} and {@code method}'s class, the proxy
     * overrides that method. The proxy is in {@code method}'s package, so the JVM treats the proxy's method as
     * overriding {@code method} too, and invoking {@code method} on the proxy runs the other method's body.
     *
     * @param method         the handler or factory method the scan found
     * @param implementation the class a CGLIB proxy in the bean's chain subclasses
     * @return the method the proxy runs instead of {@code method}, or {@code null} when it runs {@code method}
     */
    public static @Nullable Method methodACglibProxyRunsInsteadOf(Method method, Class<?> implementation) {
        int modifiers = method.getModifiers();
        if (Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers) || Modifier.isPrivate(modifiers) || Modifier.isStatic(modifiers)
                || !inSameRuntimePackage(method.getDeclaringClass(), implementation) || !method.getDeclaringClass().isAssignableFrom(implementation)) {
            return null;
        }
        for (Class<?> candidateClass = implementation; candidateClass != method.getDeclaringClass(); candidateClass = candidateClass.getSuperclass()) {
            Method candidate = declaredMethod(candidateClass, method);
            if (candidate == null || Modifier.isStatic(candidate.getModifiers()) || Modifier.isPrivate(candidate.getModifiers())) {
                continue;
            }
            boolean visibleToTheProxy = Modifier.isPublic(candidate.getModifiers()) || Modifier.isProtected(candidate.getModifiers())
                    || inSameRuntimePackage(candidateClass, implementation);
            if (visibleToTheProxy) {
                return overrides(candidate, method) ? null : candidate;
            }
        }
        return null;
    }

    // A JDK proxy hands its invocation handler one method for each name and parameter types, the one from the first
    // interface it lists that has that signature, so two annotated interface methods of one signature are one method
    // the proxy runs. MethodIntrospector gives the methods in the order the proxy lists its interfaces, so the first
    // annotated one is kept, the way an annotation search on a class implementing those interfaces finds the first.
    // When the two have different handler annotations, keeping either would let the order of the interfaces decide
    // how the handler runs, so the scan refuses them, as the post-processors refuse a method with both annotations
    private static Set<Method> firstOfEachSignature(Class<?> type, Set<Method> methods, List<Class<? extends Annotation>> annotationTypes) {
        Map<List<Object>, Method> bySignature = new LinkedHashMap<>();
        for (Method method : methods) {
            Method kept = bySignature.putIfAbsent(List.of(method.getName(), List.of(method.getParameterTypes())), method);
            if (kept != null && !handlerAnnotationsOf(kept, annotationTypes).equals(handlerAnnotationsOf(method, annotationTypes))) {
                throw new IllegalArgumentException("Method %s#%s is annotated with %s and %s#%s with %s, and JDK proxy %s runs them as one method since they have the same name and parameter types, use only one annotation."
                        .formatted(kept.getDeclaringClass().getName(), kept.getName(), String.join(" and ", handlerAnnotationsOf(kept, annotationTypes)),
                                method.getDeclaringClass().getName(), method.getName(), String.join(" and ", handlerAnnotationsOf(method, annotationTypes)), type.getName()));
            }
        }
        return new LinkedHashSet<>(bySignature.values());
    }

    private static List<String> handlerAnnotationsOf(Method method, List<Class<? extends Annotation>> annotationTypes) {
        MergedAnnotations annotations = MergedAnnotations.from(method, SearchStrategy.TYPE_HIERARCHY);
        return annotationTypes.stream().filter(annotations::isPresent).map(annotationType -> "@" + annotationType.getSimpleName()).toList();
    }

    // The method the JVM runs when method is invoked on an instance of type, bridges resolved to the method they
    // bridge. The JVM picks the first method of the same signature up from type that overrides method, and Spring's
    // ClassUtils.getMostSpecificMethod decides overriding for a package-private method by type's package instead of
    // the overriding method's, which is wrong when a superclass in method's own package overrides it
    public static Method methodTheJvmRuns(Method method, Class<?> type) {
        Class<?> declaringClass = method.getDeclaringClass();
        int modifiers = method.getModifiers();
        if (Modifier.isStatic(modifiers) || Modifier.isPrivate(modifiers)) {
            return method;
        }
        // A JDK proxy class hands every interface method to its invocation handler, so the interface method is the one
        // that runs, not the proxy class's generated method. A sub-interface the proxy implements may redeclare it, so
        // it resolves against the first interface the proxy lists that extends the method's interface
        if (declaringClass.isInterface() && java.lang.reflect.Proxy.isProxyClass(type)) {
            for (Class<?> proxiedInterface : type.getInterfaces()) {
                if (declaringClass.isAssignableFrom(proxiedInterface)) {
                    return BridgeMethodResolver.findBridgedMethod(AopUtils.getMostSpecificMethod(method, proxiedInterface));
                }
            }
            return BridgeMethodResolver.findBridgedMethod(method);
        }
        if (declaringClass.isInterface() || !declaringClass.isAssignableFrom(type)) {
            return BridgeMethodResolver.findBridgedMethod(AopUtils.getMostSpecificMethod(method, type));
        }
        for (Class<?> candidateClass = type; candidateClass != null && candidateClass != declaringClass; candidateClass = candidateClass.getSuperclass()) {
            Method candidate = declaredMethod(candidateClass, method);
            if (candidate != null && overrides(candidate, method)) {
                return BridgeMethodResolver.findBridgedMethod(candidate);
            }
        }
        return BridgeMethodResolver.findBridgedMethod(method);
    }

    // Whether overrider, declared in a subclass of overridden's declaring class, overrides it under the JVM's rules
    // (JVMS 5.4.5). A package-private method is overridden from its own runtime package, or by overriding a method in
    // between that overrides it
    private static boolean overrides(Method overrider, Method overridden) {
        if (Modifier.isPrivate(overrider.getModifiers()) || Modifier.isStatic(overrider.getModifiers())) {
            return false;
        }
        int modifiers = overridden.getModifiers();
        if (Modifier.isPrivate(modifiers) || Modifier.isStatic(modifiers)) {
            return false;
        }
        if (Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers) || inSameRuntimePackage(overrider.getDeclaringClass(), overridden.getDeclaringClass())) {
            return true;
        }
        for (Class<?> between = overrider.getDeclaringClass().getSuperclass(); between != null && between != overridden.getDeclaringClass(); between = between.getSuperclass()) {
            Method intermediate = declaredMethod(between, overridden);
            if (intermediate != null && overrides(intermediate, overridden) && overrides(overrider, intermediate)) {
                return true;
            }
        }
        return false;
    }

    // Whether two handlers of one name and parameter types, from scans of instances of one bean, are one handler. When
    // the two scans read one class, or one class extends the other, they are one handler when an instance of the class
    // further down runs one method for both. So a private method and a method of the same signature its subclass
    // declares are two handlers, and so are a private method of a superclass and an interface default method. A bean
    // whose instances come from unrelated classes has one handler of the signature, as in 0.34.0
    static boolean isSameHandler(HandlerMethod a, HandlerMethod b) {
        Class<?> below;
        if (a.implementation().isAssignableFrom(b.implementation())) {
            below = b.implementation();
        } else if (b.implementation().isAssignableFrom(a.implementation())) {
            below = a.implementation();
        } else {
            return true;
        }
        return methodTheJvmRuns(a.method(), below).equals(methodTheJvmRuns(b.method(), below));
    }

    // Whether type or a superclass of type below method's declaring class declares an instance method of method's
    // signature that isn't private
    private static boolean declaredBelow(Method method, Class<?> type) {
        for (Class<?> candidateClass = type; candidateClass != null && candidateClass != method.getDeclaringClass(); candidateClass = candidateClass.getSuperclass()) {
            Method candidate = declaredMethod(candidateClass, method);
            if (candidate != null && !Modifier.isPrivate(candidate.getModifiers()) && !Modifier.isStatic(candidate.getModifiers())) {
                return true;
            }
        }
        return false;
    }

    private static boolean inSameRuntimePackage(Class<?> a, Class<?> b) {
        return a.getPackageName().equals(b.getPackageName()) && a.getClassLoader() == b.getClassLoader();
    }

    private static @Nullable Method declaredMethod(Class<?> type, Method signature) {
        try {
            return type.getDeclaredMethod(signature.getName(), signature.getParameterTypes());
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    /**
     * Answers whether {@code beanName} names the proxy of a scoped bean, a definition that decorates its target's
     * definition under {@link ScopedProxyUtils#getTargetBeanName}. Both annotation post-processors skip such a name,
     * so a scoped bean's handlers register once, under its target's name. Internal to the Spring Boot starters.
     *
     * @param beanFactory the bean factory that defines {@code beanName}
     * @param beanName    the bean name the scan is about to read
     * @return {@code true} when {@code beanName} is a scoped proxy whose target has a definition of its own
     */
    public static boolean isScopedProxy(ConfigurableListableBeanFactory beanFactory, String beanName) {
        if (!beanFactory.containsBeanDefinition(beanName)) {
            return false;
        }
        String targetBeanName = ScopedProxyUtils.getTargetBeanName(beanName);
        BeanDefinitionHolder decorated = beanFactory.getMergedBeanDefinition(beanName) instanceof RootBeanDefinition root ? root.getDecoratedDefinition() : null;
        return decorated != null && decorated.getBeanName().equals(targetBeanName) && beanFactory.containsBeanDefinition(targetBeanName);
    }

    private static IllegalStateException annotationFromAMethodNotOverridden(Method method, Method declaration, MergedAnnotation<? extends Annotation> annotation) {
        String annotationName = "@" + annotation.getType().getSimpleName();
        String handler = annotation.getValue("id", String.class).map(id -> "%s with id \"%s\"".formatted(annotationName, id)).orElse(annotationName);
        if (Modifier.isStatic(method.getModifiers())) {
            return new IllegalStateException(("Method %s is static, so it does not override %s, which declares %s, but Spring's annotation search finds the annotation on both, so which body is the handler is ambiguous. "
                    + "Rename %s to keep %s as the handler.").formatted(signatureOf(method), signatureOf(declaration), handler, signatureOf(method), signatureOf(declaration)));
        }
        return new IllegalStateException(("Method %s does not override %s, which declares %s, but Spring's annotation search finds the annotation on both, so which body is the handler is ambiguous. "
                + "Make both %s and %s protected or public so %s overrides it and runs as the handler, or rename %s to keep %s as the handler.").formatted(
                signatureOf(method), signatureOf(declaration), handler, signatureOf(declaration), signatureOf(method), signatureOf(method), signatureOf(method), signatureOf(declaration)));
    }

    private static String signatureOf(Method method) {
        return method.getDeclaringClass().getName() + '#' + method.getName()
                + Arrays.stream(method.getParameterTypes()).map(Class::getSimpleName).collect(Collectors.joining(", ", "(", ")"));
    }
}
