# dp-poc

이벤트 기반 데이터 플랫폼의 핵심 컴포넌트를 구현한 PoC(Proof of Concept) 프로젝트입니다.

## 🎯 주요 기능

이 프로젝트는 3개의 핵심 시스템으로 구성되어 있습니다:

1. **Event System** 🔔 - 비동기 이벤트 메시징 시스템
2. **Scheduler System** ⏰ - 다양한 트리거 기반 스케줄링 시스템
3. **Task Registry** 📋 - Task 등록 및 메타데이터 관리 시스템

### Event System
- 토픽 기반 pub/sub 패턴
- 와일드카드 패턴 매칭 (`*`, `**`)
- 비동기 제너레이터 기반 이벤트 스트림
- Hexagonal Architecture (포트 & 어댑터)

### Scheduler System
- **CronTrigger**: Crontab 기반 스케줄링
- **APITrigger**: API 호출 기반 트리거
- **EventTrigger**: 이벤트 수신 기반 트리거
- FastAPI 통합 지원

### Task Registry
- 동기/비동기 함수 지원
- 자동 스키마 추출 (타입 힌트 기반)
- Task 간 의존성 관리
- 태그 기반 분류 및 조회

## 📦 프로젝트 구조

```
dp-poc/
├── app/
│   ├── event_system/           # 이벤트 시스템
│   │   ├── domain/            # 포트 인터페이스
│   │   │   ├── broker_port.py
│   │   │   ├── publisher_port.py
│   │   │   ├── consumer_port.py
│   │   │   ├── queue_port.py
│   │   │   └── events.py
│   │   ├── infrastructure/    # 어댑터 구현
│   │   │   ├── in_memory_broker.py
│   │   │   ├── in_memory_publisher.py
│   │   │   ├── in_memory_consumer.py
│   │   │   └── in_memory_queue.py
│   │   └── utils/
│   │       └── topic_matcher.py
│   │
│   ├── scheduler/              # 스케줄러 시스템
│   │   ├── domain/            # 포트 인터페이스
│   │   │   ├── trigger_port.py
│   │   │   └── scheduler_port.py
│   │   ├── infrastructure/    # 트리거 구현
│   │   │   ├── cron_trigger.py
│   │   │   ├── api_trigger.py
│   │   │   ├── event_trigger.py
│   │   │   ├── manual_trigger.py
│   │   │   ├── scheduler.py
│   │   │   └── trigger_factory.py
│   │   ├── api/               # FastAPI 통합
│   │   │   ├── fastapi_integration.py
│   │   │   └── example_fastapi.py
│   │   └── README.md
│   │
│   ├── task_registry/          # Task Registry 시스템
│   │   ├── domain/            # 도메인 모델
│   │   │   ├── task_model.py
│   │   │   └── registry_port.py
│   │   ├── infrastructure/    # Registry 구현
│   │   │   └── task_registry.py
│   │   ├── utils/             # 유틸리티
│   │   │   └── schema_utils.py
│   │   ├── decorator.py       # @task 데코레이터
│   │   └── README.md
│   │
│   └── main.py                # 메인 진입점
│
├── examples/                   # 예제 코드
│   ├── schema_extraction_demo.py
│   └── task_registry_example.py
│
├── tests/                      # 테스트
│   ├── event_system/
│   ├── scheduler/
│   ├── task_registry/
│   └── benchmark/
│
├── pyproject.toml
├── Makefile
└── README.md
```

## 🚀 빠른 시작

### 필수 요구사항
- Python 3.12+
- [uv](https://github.com/astral-sh/uv) (Python 패키지 관리자)

### 설치

```bash
# 개발 환경 설정 (가상환경 생성 + 의존성 설치)
make dev

# 또는 의존성만 설치
make install
```

### 기본 실행

```bash
# 메인 데모 실행
make run

# 패턴 매칭 데모
python -m app.main --pattern

# Task Registry 예제
python -m examples.task_registry_example

# Schema 추출 데모
python -m examples.schema_extraction_demo
```

## 💡 사용 예제

### 1. Event System 사용하기

```python
from app.event_system.domain.events import EventBase
from app.event_system.infrastructure import InMemoryBroker, InMemoryPublisher, InMemoryConsumer
import asyncio

# Setup
broker = InMemoryBroker()
publisher = InMemoryPublisher(broker)
consumer = InMemoryConsumer(broker)

# Event 정의
class DataEvent(EventBase):
    def __init__(self, topic: str, data: dict, **kwargs):
        super().__init__(topic=topic, data=data, **kwargs)
        self.data = data

# Publish
await publisher.publish("data.ingestion", DataEvent(topic="data.ingestion", data={"rows": 100}))

# Consume
async for event in consumer.consume("data.ingestion"):
    print(f"Received: {event.data}")
```

### 2. Scheduler 사용하기

```python
from app.scheduler import Scheduler, create_cron_trigger, create_api_trigger

# Cron 기반 스케줄링
cron_trigger = create_cron_trigger(
    cron_expression="0 9 * * *",  # 매일 오전 9시
    event_factory=lambda: DailyReportEvent(topic="reports.daily")
)

scheduler = Scheduler(publisher)
await scheduler.register_trigger("daily_report", cron_trigger, "reports.daily")
await scheduler.start()
```

### 3. Task Registry 사용하기

```python
from app.task_registry import task, get_registry

# Task 등록
@task(name="extract_data", tags=["etl", "extract"])
def extract_data() -> dict[str, list[int]]:
    return {"data": [1, 2, 3, 4, 5]}

@task(name="transform_data", tags=["etl", "transform"], dependencies=["extract_data"])
async def transform_data(data: dict[str, list[int]]) -> list[int]:
    return [x * 2 for x in data["data"]]

# Registry 조회
registry = get_registry()
task_info = registry.get("extract_data")
print(f"Task: {task_info.name}, Async: {task_info.is_async}")
```

## 🧪 테스트

```bash
# 모든 테스트 실행
make test

# 커버리지와 함께 테스트
make test-cov

# 특정 테스트만 실행
pytest tests/event_system/ -v
pytest tests/scheduler/ -v
pytest tests/task_registry/ -v

# 벤치마크 테스트
make test-benchmark
```

## 🛠️ 개발 도구

### 코드 품질 도구

- **ruff**: Python linter 및 formatter
- **mypy**: 정적 타입 체커
- **isort**: import 정렬
- **pre-commit**: Git pre-commit 훅

### Make 명령어

```bash
# 코드 품질
make lint          # 코드 린팅 (자동 수정)
make format        # 코드 포맷팅
make type-check    # 타입 체크
make pre-commit    # pre-commit 실행 (모든 파일)

# Pre-commit 설정
make pre-commit-install  # pre-commit 훅 설치

# 테스트
make test          # 모든 테스트 실행
make test-cov      # 커버리지와 함께 테스트 실행
make test-unit     # 유닛 테스트만 실행
make test-integration  # 통합 테스트만 실행
make test-benchmark    # 벤치마크 테스트만 실행

# 실행
make run           # 애플리케이션 실행
```

## 📚 상세 문서

각 시스템의 상세한 사용법은 아래 문서를 참고하세요:

- [Event System](app/event_system/) - 이벤트 메시징 시스템
- [Scheduler System](app/scheduler/README.md) - 스케줄링 시스템
- [Task Registry](app/task_registry/README.md) - Task 관리 시스템

## 🏗️ 아키텍처

### Hexagonal Architecture (포트 & 어댑터)

프로젝트는 Hexagonal Architecture를 따릅니다:

- **Domain Layer** (`domain/`): 비즈니스 로직과 포트(인터페이스) 정의
- **Infrastructure Layer** (`infrastructure/`): 어댑터(구현체)
- **API Layer** (`api/`): 외부 인터페이스 (FastAPI 등)

이 구조는 비즈니스 로직을 인프라 구현으로부터 완전히 분리하여:
- 테스트 용이성 향상
- 구현체 교체 용이 (In-Memory → Redis, Kafka 등)
- 의존성 역전 원칙 준수

## 🔄 통합 예제

### 전체 데이터 파이프라인

```python
from app.event_system.infrastructure import InMemoryBroker, InMemoryPublisher, InMemoryConsumer
from app.scheduler import Scheduler, create_cron_trigger, create_event_trigger
from app.task_registry import task

# Event System 설정
broker = InMemoryBroker()
publisher = InMemoryPublisher(broker)
consumer = InMemoryConsumer(broker)

# Task 정의
@task(name="extract", tags=["etl"])
async def extract_data():
    data = {"rows": [1, 2, 3, 4, 5]}
    await publisher.publish("data.extracted", DataExtractedEvent(topic="data.extracted", data=data))
    return data

@task(name="transform", tags=["etl"], dependencies=["extract"])
async def transform_data(data: dict):
    transformed = [x * 2 for x in data["rows"]]
    await publisher.publish("data.transformed", DataTransformedEvent(topic="data.transformed", data=transformed))
    return transformed

# Scheduler 설정
scheduler = Scheduler(publisher)

# 1. 매일 오전 3시에 데이터 추출 시작
extract_trigger = create_cron_trigger(
    cron_expression="0 3 * * *",
    event_factory=lambda: StartExtractEvent(topic="pipeline.start")
)

# 2. 추출 완료 시 자동으로 변환 시작
transform_trigger = create_event_trigger(
    consumer=consumer,
    source_topic="data.extracted",
    event_factory=lambda event: StartTransformEvent(topic="pipeline.transform", data=event.data)
)

# 등록 및 시작
await scheduler.register_trigger("daily_extract", extract_trigger, "pipeline.start")
await scheduler.register_trigger("auto_transform", transform_trigger, "pipeline.transform")
await scheduler.start()
```

## 📈 성능

벤치마크 테스트 결과 (Apple M1, Python 3.12):

- **Event Throughput**: ~100K events/sec
- **Pattern Matching**: ~50K matches/sec
- **Task Registry**: ~1M lookups/sec

자세한 벤치마크는 `tests/benchmark/` 참고

## 🔮 향후 계획

- [ ] Redis/Kafka 어댑터 구현
- [ ] Task DAG 실행 엔진
- [ ] 분산 스케줄링 지원
- [ ] 모니터링 및 메트릭
- [ ] UI 대시보드

## 📝 라이센스

이 프로젝트는 사내 PoC 프로젝트입니다.

## 🤝 기여

내부 팀원들의 기여를 환영합니다. PR을 올리기 전에:

1. `make lint` 실행
2. `make test` 통과 확인
3. 새 기능은 테스트 코드 포함
4. 문서 업데이트
