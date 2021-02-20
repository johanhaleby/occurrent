<img src="https://raw.githubusercontent.com/johanhaleby/occurrent/master/occurrent-logo-196x196.png" width="128" height="128"></img>
<br>
[![Build Status](https://github.com/johanhaleby/occurrent/actions/workflows/maven.yml/badge.svg)](https://github.com/johanhaleby/occurrent/actions/workflows/maven.yml)

Occurrent is a set of Event Sourcing utilities based on the [cloud events](https://cloudevents.io/) specification. 

Work in progress, don't use in production just yet :)

#### Documentation

You can find documentation on the [website](https://occurrent.org).

#### Design Choices

Occurrent is designed to be [simple](https://www.infoq.com/presentations/Simple-Made-Easy/), non-intrusive and pragmatic. It emphasises understandability, composability, transparentness and pragmatism.
 
* You should be able to design your domain model without _any_ dependencies to Occurrent or any other library. Your domain model can be expressed with pure functions that returns events. Use Occurrent to store these events.
* Simple: Pick only the libraries you need, no need for an all or nothing solution.
* You should be in control! Magic is kept to a minimum and data is stored in a standard format ([cloud events](https://cloudevents.io/)). You are responsible for serializing/deserializing the cloud events "body" (data) yourself.
* Composable: Function composition and pipes are encouraged. For example pipe the event stream to a rehydration function (any function that converts a stream of events to current state) before calling your domain model.
* Designed to be used as a library and not a framework to the greatest extent possible.
* Pragmatic: Need consistent projections? You can decide to write projections and events transactionally using tools you already know (such as Spring `@Transactional`)! 
* Interoperable/Portable: Cloud events is a [CNCF](https://www.cncf.io/) specification for describing event data in a common way. CloudEvents seeks to dramatically simplify event declaration and delivery across services, platforms, and beyond!
* Use the Occurrent components as lego bricks to compose your own pipelines. Components are designed to be small so that you should be able to re-write them tailored to your own needs if required. Missing a component? You should be able to write one yourself and hook into the rest of the eco-system. Write your own problem/domain specific layer on-top of Occurrent.
* Since you know that events are stored as Cloud Events even in the database you can use the database to your advantage. For example you can create custom indexes used for fast and fully consistent domain queries directly on an event stream (or even multiple streams).
