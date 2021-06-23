Overview
========

Concepts
--------
* StoreAdapter - the place where events are stored.
* Store - a domain object allowing access reading (and possibly writing).
* CommandProcessor - Pre-commit event auditor.
* Stream - An ongoing, async series of events.
* Aggregate - a disjoint subset of the full stream.
* Projection - a queryable representation of the result of applied events.
* Dispatcher - an entity that broadcasts events to many subscribers.
* Event - The atom of the event store.
* Commit - An event containing multiple events that occur simultaneously.

Event Context vs Commit/Command Context
--------------------------------

* Store itself is event-centric.
* Event dispatchers can be a thing.

* Projections are commit-centric.
* Dispatcher can be commit-centric or event-centric.
* end-user interactions should be commit-centric.
* Commits themselves can be modeled as events. Therefore the "actual" events would be sub-events.
* In this context, a commit is an event approved by a command processor.


### Open questions

-
