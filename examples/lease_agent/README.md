# Lease Agent

Демонстрация `WithRunLease` + `MemoryLeaseManager` и `WithDeleteOnSuccess(true)`.

## Сценарий

1. `Start` с lease — suspend после первого шага.
2. `Resume` с тем же owner — завершение run.
3. Checkpoint удаляется после успешного завершения (delete-on-success + release lease).

`ErrLeaseLost` / TTL takeover покрыты unit-тестами (`runner_lifecycle_test.go`), не в этом example.

## Запуск

```bash
cd examples/lease_agent
go run .
```
