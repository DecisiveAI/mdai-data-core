# Current changes
### Audit Adapter
1. Refactor HandleEventsGet to match new endpoint structure in 
   `event-handler-webservice`
### Variables Adapter
2. Add ctx to `ValKeyAdapter` struct to support cmd execute
3. Add debug logs, cmd execute to `AddElementToSet`
4. Refactor log structures -- normalized on structured logs via `zap`

## TODOs:
1. Add debug logs, cmd execute to remaining methods
2. Add Audit logs to remaining methods
4. Review audit adapter, some logic could be outdated
5. Update tests to cover handlers package
6. Merge to main and push a version tag
7. Update all dependent projects with new tag