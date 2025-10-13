# Scheduler System

이벤트 기반 스케줄러 시스템으로, 다양한 트리거 조건에 따라 이벤트를 자동으로 발행합니다.

## 🎯 주요 기능

### 3가지 트리거 타입

1. **CronTrigger** ⏰ - Crontab 기반 스케줄링
2. **APITrigger** 🔌 - API 호출 기반 트리거
3. **EventTrigger** 🔔 - 이벤트 수신 기반 트리거

## 📦 아키텍처

```
app/scheduler/
├── domain/               # 도메인 레이어 (Ports)
│   ├── trigger_port.py   # 트리거 인터페이스
│   └── scheduler_port.py # 스케줄러 인터페이스
└── infrastructure/       # 인프라 레이어 (Adapters)
    ├── cron_trigger.py   # Cron 트리거 구현
    ├── api_trigger.py    # API 트리거 구현
    ├── event_trigger.py  # Event 트리거 구현
    ├── scheduler.py      # 스케줄러 구현
    └── trigger_factory.py # 팩토리 패턴
```

## 🚀 빠른 시작

### 1. CronTrigger 사용

```python
from app.scheduler import create_cron_trigger, Scheduler
from app.event_system.infrastructure import InMemoryBroker, InMemoryPublisher

# Setup
broker = InMemoryBroker()
publisher = InMemoryPublisher(broker)
scheduler = Scheduler(publisher)

# Create cron trigger - every 5 minutes
cron_trigger = create_cron_trigger(
    cron_expression="*/5 * * * *",
    event_factory=lambda: MyEvent(topic="scheduled.reports"),
)

# Register and start
await scheduler.register_trigger("daily_report", cron_trigger, "scheduled.reports")
await scheduler.start()
```

**Cron Expression 예제:**
```python
"* * * * *"        # 매 분
"0 * * * *"        # 매 시간
"0 0 * * *"        # 매일 자정
"0 9 * * MON-FRI"  # 평일 오전 9시
"*/15 * * * *"     # 15분마다
```

### 2. APITrigger 사용

#### 방법 1: 수동으로 trigger() 호출

```python
from app.scheduler import create_api_trigger

# Create API trigger
api_trigger = create_api_trigger(
    event_factory=lambda payload: WebhookEvent(
        topic="webhooks.incoming",
        webhook_id=payload.get("webhook_id"),
        data=payload
    ),
)

await scheduler.register_trigger("webhook_handler", api_trigger, "webhooks.incoming")
await scheduler.start()

# Later, trigger from anywhere in your code
await api_trigger.trigger({
    "webhook_id": "wh_001",
    "user_id": 123,
    "action": "purchase"
})
```

#### 방법 2: FastAPI와 통합 (권장) 🔥

```python
from fastapi import FastAPI
from app.scheduler.api import create_trigger_router

app = FastAPI()

# Create trigger
api_trigger = create_api_trigger(event_factory)
await scheduler.register_trigger("webhook", api_trigger, "webhooks.incoming")

# Add API routes automatically
router = create_trigger_router(api_trigger, prefix="/api/webhooks")
app.include_router(router)

# Now you have:
# POST /api/webhooks/trigger - Trigger events
# GET  /api/webhooks/status  - Check trigger status
```

**API 호출 예제:**
```bash
# Trigger an event via HTTP POST
curl -X POST http://localhost:8000/api/webhooks/trigger \
  -H "Content-Type: application/json" \
  -d '{"data": {"webhook_id": "wh_001", "user_id": 123, "action": "login"}}'

# Check trigger status
curl http://localhost:8000/api/webhooks/status
```

자세한 내용은 [FastAPI 통합 가이드](#fastapi-통합)를 참고하세요.

### 3. EventTrigger 사용

```python
from app.scheduler import create_event_trigger

# Create event trigger - auto-start processing when data arrives
event_trigger = create_event_trigger(
    consumer=consumer,
    source_topic="data.ingestion",
    event_factory=lambda source_event: ProcessingEvent(
        topic="data.processing",
        source_id=source_event.source_name
    ),
    filter_func=lambda event: event.rows_count > 1000  # Only trigger for large datasets
)

await scheduler.register_trigger("auto_processing", event_trigger, "data.processing")
await scheduler.start()
```

## 🔧 Factory 패턴 사용

### 방법 1: TriggerFactory 직접 사용

```python
from app.scheduler import TriggerFactory, TriggerType

trigger = TriggerFactory.create_trigger(
    TriggerType.CRON,
    cron_expression="0 0 * * *",
    event_factory=lambda: MyEvent(topic="daily")
)
```

### 방법 2: 문자열로 타입 지정

```python
trigger = TriggerFactory.create_trigger(
    "cron",  # 문자열도 가능
    cron_expression="0 0 * * *",
    event_factory=lambda: MyEvent(topic="daily")
)
```

### 방법 3: Convenience 함수 사용 (권장)

```python
from app.scheduler import create_cron_trigger, create_api_trigger, create_event_trigger

# 가장 간단하고 타입 안전한 방법
cron = create_cron_trigger("0 0 * * *", event_factory)
api = create_api_trigger(event_factory)
event = create_event_trigger(consumer, "source.topic", event_factory)
```

## 📊 Scheduler 관리

### 트리거 등록 및 관리

```python
scheduler = Scheduler(publisher)

# 트리거 등록
await scheduler.register_trigger("trigger_1", cron_trigger, "topic.1")
await scheduler.register_trigger("trigger_2", api_trigger, "topic.2")

# 스케줄러 시작 (모든 트리거 동시 시작)
await scheduler.start()

# 특정 트리거 제거
await scheduler.unregister_trigger("trigger_1")

# 등록된 트리거 확인
triggers = scheduler.get_registered_triggers()
print(f"Active triggers: {list(triggers.keys())}")

# 스케줄러 종료 (모든 트리거 정리)
await scheduler.stop()
```

### 동적 트리거 추가

```python
# 스케줄러 실행 중에도 트리거 추가 가능
await scheduler.start()

# 나중에 새 트리거 추가
new_trigger = create_cron_trigger("0 12 * * *", lunch_event_factory)
await scheduler.register_trigger("lunch_reminder", new_trigger, "reminders.lunch")
# 자동으로 시작됨!
```

## 🎨 실전 예제

### 예제 1: 데이터 파이프라인 자동화

```python
# 1. 매일 새벽 3시에 데이터 수집 시작
data_collection_trigger = create_cron_trigger(
    cron_expression="0 3 * * *",
    event_factory=lambda: DataCollectionEvent(
        topic="pipeline.collection",
        source="external_api"
    )
)

# 2. 데이터 수집 완료 시 자동으로 처리 시작
data_processing_trigger = create_event_trigger(
    consumer=consumer,
    source_topic="pipeline.collection.completed",
    event_factory=lambda source: DataProcessingEvent(
        topic="pipeline.processing",
        data_id=source.data_id
    )
)

# 3. 처리 완료 시 알림 발송
notification_trigger = create_event_trigger(
    consumer=consumer,
    source_topic="pipeline.processing.completed",
    event_factory=lambda source: NotificationEvent(
        topic="notifications.email",
        recipient="admin@example.com",
        message=f"Pipeline completed: {source.data_id}"
    )
)

# 모두 등록
await scheduler.register_trigger("daily_collection", data_collection_trigger, "pipeline.collection")
await scheduler.register_trigger("auto_processing", data_processing_trigger, "pipeline.processing")
await scheduler.register_trigger("completion_notify", notification_trigger, "notifications.email")

await scheduler.start()
```

### 예제 2: 실시간 모니터링 시스템

```python
# 에러 발생 시 즉시 알림
error_alert_trigger = create_event_trigger(
    consumer=consumer,
    source_topic="logs.**",  # 모든 로그 토픽 감시
    event_factory=lambda log_event: AlertEvent(
        topic="alerts.critical",
        level="CRITICAL",
        message=f"Error in {log_event.service}: {log_event.error_message}"
    ),
    filter_func=lambda event: event.level == "ERROR"  # ERROR 레벨만 필터링
)

await scheduler.register_trigger("error_alerting", error_alert_trigger, "alerts.critical")
```

### 예제 3: API Webhook 처리

```python
# FastAPI endpoint example
from fastapi import FastAPI

app = FastAPI()

# API trigger 생성
webhook_trigger = create_api_trigger(
    event_factory=lambda payload: WebhookReceivedEvent(
        topic="webhooks.github",
        repository=payload.get("repository"),
        action=payload.get("action"),
        data=payload
    )
)

await scheduler.register_trigger("github_webhooks", webhook_trigger, "webhooks.github")
await scheduler.start()

@app.post("/webhooks/github")
async def handle_github_webhook(payload: dict):
    # API trigger 발동
    await webhook_trigger.trigger(payload)
    return {"status": "processed"}
```

## ⚙️ 고급 기능

### 타임존 설정 (CronTrigger)

```python
cron_trigger = create_cron_trigger(
    cron_expression="0 9 * * MON-FRI",
    event_factory=event_factory,
    timezone="Asia/Seoul"  # 한국 시간 기준
)
```

### 복잡한 필터링 (EventTrigger)

```python
# 여러 조건을 조합한 복잡한 필터
event_trigger = create_event_trigger(
    consumer=consumer,
    source_topic="data.events",
    event_factory=process_factory,
    filter_func=lambda event: (
        event.priority == "HIGH" and
        event.source in ["api", "batch"] and
        event.timestamp > datetime.now() - timedelta(hours=1)
    )
)
```

### 트리거 상태 모니터링

```python
# API trigger 호출 횟수 확인
count = api_trigger.get_trigger_count()
print(f"Triggered {count} times")

# Event trigger 호출 횟수 확인
count = event_trigger.get_trigger_count()
print(f"Processed {count} events")

# 트리거 실행 상태 확인
if cron_trigger.is_running():
    print("Cron trigger is active")
```

## 🧪 테스트

```bash
# 예제 실행
uv run python -m app.scheduler.examples

# 단위 테스트 (곧 추가 예정)
uv run pytest tests/test_scheduler.py -v
```

## 📝 API Reference

### TriggerPort (추상 인터페이스)

```python
async def start() -> None
async def stop() -> None
def emit() -> AsyncGenerator[E, None]
def is_running() -> bool
```

### SchedulerPort (추상 인터페이스)

```python
async def register_trigger(trigger_id: str, trigger: TriggerPort[E], topic: str) -> None
async def unregister_trigger(trigger_id: str) -> None
async def start() -> None
async def stop() -> None
def get_registered_triggers() -> dict[str, tuple[TriggerPort[E], str]]
```

## 🔍 트러블슈팅

### Q: Cron trigger가 동작하지 않아요
A: cron expression이 올바른지 확인하세요. [crontab.guru](https://crontab.guru/)에서 검증 가능합니다.

### Q: Event trigger가 이벤트를 받지 못해요
A:
- source_topic이 정확한지 확인
- 패턴 매칭 사용 시 `*` 또는 `**` 올바르게 사용
- filter_func이 너무 엄격하지 않은지 확인

### Q: API trigger를 어떻게 호출하나요?
A:
```python
await api_trigger.trigger({"your": "data"})
```
trigger 인스턴스를 저장해두고 필요할 때 호출하세요.

## 🌐 FastAPI 통합

### 완전한 예제

```python
from fastapi import FastAPI
from contextlib import asynccontextmanager

from app.scheduler import Scheduler, create_api_trigger
from app.scheduler.api import create_trigger_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global scheduler, api_trigger

    scheduler = Scheduler(publisher)
    api_trigger = create_api_trigger(event_factory)

    await scheduler.register_trigger("webhook", api_trigger, "webhooks")
    await scheduler.start()

    yield

    # Shutdown
    await scheduler.stop()


app = FastAPI(lifespan=lifespan)

# Add trigger routes
router = create_trigger_router(api_trigger, prefix="/api/webhooks")
app.include_router(router)
```

### 실행 및 테스트

```bash
# FastAPI 서버 실행
uv run python -m app.scheduler.api.example_fastapi

# API 문서 확인
open http://localhost:8000/docs

# 이벤트 트리거
curl -X POST http://localhost:8000/api/webhooks/trigger \
  -H "Content-Type: application/json" \
  -d '{
    "data": {
      "webhook_id": "wh_001",
      "user_id": 123,
      "action": "purchase"
    }
  }'

# 상태 확인
curl http://localhost:8000/api/webhooks/status
```

### 생성되는 API 엔드포인트

#### POST `/api/webhooks/trigger`
이벤트를 트리거합니다.

**Request:**
```json
{
  "data": {
    "webhook_id": "wh_001",
    "user_id": 123,
    "action": "login"
  }
}
```

**Response:**
```json
{
  "success": true,
  "message": "Event triggered successfully",
  "trigger_count": 42
}
```

#### GET `/api/webhooks/status`
트리거 상태를 확인합니다.

**Response:**
```json
{
  "running": true,
  "trigger_count": 42
}
```

### 커스텀 엔드포인트 추가

```python
from fastapi import FastAPI

app = FastAPI()

# APITrigger 인스턴스를 직접 사용
@app.post("/webhooks/github")
async def github_webhook(payload: dict):
    # GitHub webhook 처리
    webhook_data = {
        "webhook_id": f"gh_{payload.get('hook_id')}",
        "action": payload.get("action"),
    }

    await api_trigger.trigger(webhook_data)

    return {"status": "processed"}
```

## 📚 더 알아보기

- [Event System 문서](../event_system/README.md)
- [예제 코드](./examples.py)
- [FastAPI 예제](./api/example_fastapi.py)
- [Cron 표현식 가이드](https://en.wikipedia.org/wiki/Cron)
