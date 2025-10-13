# APITrigger 동작 원리

## 🤔 질문: API를 받는 부분이 없는데 어떻게 동작하나요?

`APITrigger`는 **수동 트리거** 방식입니다. HTTP 서버가 내장되어 있지 않으며, 외부에서 명시적으로 `trigger()` 메서드를 호출해야 합니다.

## 🔄 동작 흐름

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐      ┌────────────┐
│  HTTP API   │      │  APITrigger  │      │  Scheduler  │      │   Queue    │
│  (FastAPI)  │      │              │      │             │      │  (Topic)   │
└──────┬──────┘      └──────┬───────┘      └──────┬──────┘      └─────┬──────┘
       │                    │                     │                    │
       │  1. HTTP POST      │                     │                    │
       ├───────────────────>│                     │                    │
       │  /api/trigger      │                     │                    │
       │                    │                     │                    │
       │  2. trigger(data)  │                     │                    │
       ├───────────────────>│                     │                    │
       │                    │                     │                    │
       │                    │ 3. Create Event     │                    │
       │                    │ & Put in Queue      │                    │
       │                    ├────────────────────>│                    │
       │                    │                     │                    │
       │                    │                     │ 4. Publish Event   │
       │                    │                     ├───────────────────>│
       │                    │                     │                    │
       │  5. Response       │                     │                    │
       │<───────────────────┤                     │                    │
       │                    │                     │                    │
```

## 💡 핵심 개념

### 1. APITrigger는 "이벤트 생성기"
- HTTP 서버가 **아님**
- 외부에서 호출하면 이벤트를 생성하는 역할

### 2. FastAPI가 "HTTP 받는 부분"
- FastAPI/Flask 등이 HTTP 요청 수신
- 받은 데이터로 `api_trigger.trigger(data)` 호출

### 3. Scheduler가 "중개자"
- Trigger에서 생성된 이벤트를 받아서
- Publisher를 통해 Topic에 발행

## 📝 코드로 이해하기

### 단계 1: APITrigger 생성
```python
api_trigger = APITrigger(
    event_factory=lambda payload: MyEvent(
        topic="webhooks",
        data=payload
    )
)
```
- 이 시점에는 아무것도 수신하지 않음
- 그냥 "준비" 상태

### 단계 2: Scheduler에 등록
```python
scheduler.register_trigger("my_trigger", api_trigger, "webhooks")
await scheduler.start()
```
- Scheduler가 `api_trigger.emit()`을 감시하기 시작
- 이벤트가 발생하면 자동으로 발행

### 단계 3: FastAPI로 HTTP 받기
```python
from fastapi import FastAPI

app = FastAPI()

@app.post("/webhooks")
async def receive_webhook(data: dict):
    # 여기가 "API를 받는 부분"!
    await api_trigger.trigger(data)
    return {"status": "ok"}
```

### 단계 4: 내부 동작
```python
# APITrigger.trigger() 내부
async def trigger(self, payload):
    event = self.event_factory(payload)  # 1. 이벤트 생성
    await self._queue.put(event)          # 2. 내부 큐에 넣기

# APITrigger.emit() (Scheduler가 자동 호출)
async def emit(self):
    while self._running:
        event = await self._queue.get()   # 3. 큐에서 꺼내기
        yield event                        # 4. Scheduler에게 전달

# Scheduler (자동)
async def _process_trigger(trigger, topic):
    async for event in trigger.emit():
        await publisher.publish(topic, event)  # 5. Topic에 발행
```

## 🔥 2가지 사용 방법

### 방법 1: 수동 호출 (간단)
```python
# 어디서든 직접 호출
await api_trigger.trigger({"user_id": 123})
```
- 장점: 간단함
- 단점: HTTP API 직접 구현 필요

### 방법 2: FastAPI 통합 (권장)
```python
from app.scheduler.api import create_trigger_router

# 자동으로 HTTP 엔드포인트 생성
router = create_trigger_router(api_trigger, prefix="/api/webhooks")
app.include_router(router)
```
- 장점: HTTP API 자동 생성, Swagger 문서 포함
- 단점: FastAPI 의존성

## 🆚 다른 트리거와의 차이

| Trigger | 이벤트 발생 시점 | 외부 입력 필요 | HTTP 서버 |
|---------|------------------|----------------|-----------|
| **CronTrigger** | 시간 기반 (자동) | ❌ | ❌ |
| **EventTrigger** | 다른 이벤트 수신 시 | ❌ | ❌ |
| **APITrigger** | `trigger()` 호출 시 | ✅ | ❌ (별도 필요) |

## 💻 완전한 예제

```python
from fastapi import FastAPI
from app.scheduler import Scheduler, create_api_trigger
from app.scheduler.api import create_trigger_router

# 1. Setup
app = FastAPI()
scheduler = Scheduler(publisher)

# 2. Create trigger
api_trigger = create_api_trigger(
    event_factory=lambda data: WebhookEvent(**data)
)

# 3. Register
await scheduler.register_trigger("webhooks", api_trigger, "webhooks.incoming")
await scheduler.start()

# 4. Add HTTP routes (이게 "API 받는 부분")
router = create_trigger_router(api_trigger, prefix="/webhooks")
app.include_router(router)

# 5. HTTP 요청 → trigger() 호출 → 이벤트 발행
```

## 🎯 요약

1. **APITrigger 자체는 HTTP를 받지 않음**
2. **FastAPI/Flask가 HTTP를 받음**
3. **받은 데이터로 `trigger()` 호출**
4. **Scheduler가 자동으로 이벤트 발행**

→ **분리된 책임** (Separation of Concerns) 원칙! 🎉
