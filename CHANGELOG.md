# dagather Changelog
## 0.1.0
### Removed
* The `ErrorHandler` class has been removed, use `PostErrorResult`'s `exception_handler` class method.
### Changed
* `PostErrorResult` now split into two classes: `PropagateError` and `ContinueResult`
* `cancel_not_started` and `cancel_children` renamed to `discard_not_started` and `discard_children`, differentiate from asyncio cancellation
### Added
* new context variable, `sibling_tasks`, that holds a special object allowing cancellation and introspection into the current Dagather runtime
* Dagather objects now return `DagatherResult` that allow introspection.
* new cancel policy: `cancel_all`, to raise cancellation in running tasks as well as discarding waiting ones (default for `PropagateError`).
* Exception handlers can now be mappings from exception types to other handlers, handling methods by their exception type.
### Fixed
* Error handlers can now catch any exception type (although only explicitly)
## 0.0.1
* initial release