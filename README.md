### Delay-Notifier-Service

#### Описание

Сервис для отправки отложенных уведомлений через RabbitMQ

#### Сервис поддерживает такие CRUD-ы:
* POST /notify — создание уведомлений с датой и временем отправки;
* GET /notify/{id} — получение статуса уведомления;
* DELETE /notify/{id} — отмена запланированного уведомления.

#### Запуск

```
docker compose up -d
```
#### Пример curl-запроса

```
curl -X POST -H "Content-Type: application/json" -d '{"id":"2","message":"Second notification text message","send_date":"2026-06-29T22:11:00+03:00", "status":"NotSent"}' http://localhost:8081/notify
```