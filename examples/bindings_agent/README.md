# Bindings Agent

Демонстрация typed `BindingKey[T]` + `Bind` / `BindingFromContext`.

## Сценарий

1. Создать `RunBindings`, привязать зависимость через `Bind`.
2. Передать `WithBindings` в `Start`.
3. Узел извлекает зависимость из `ctx` без глобальных синглтонов.

Bindings не персистятся в snapshot.

## Запуск

```bash
cd examples/bindings_agent
go run .
```
