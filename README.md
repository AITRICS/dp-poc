# dp-poc

이벤트 기반 데이터 플랫폼의 핵심 컴포넌트를 구현한 PoC(Proof of Concept) 프로젝트입니다.

## 🆕 최근 업데이트

### 2025-01-15: I/O Manager & MultiprocessQueue 구현
- ✅ **I/O Manager 시스템**: Task 결과 저장/로딩을 위한 추상 인터페이스 구현
  - `FilesystemIOManager`: 로컬 파일 시스템 기반 구현 (Pickle 직렬화)
  - Run ID 기반 격리 및 스트리밍 출력 지원
- ✅ **MultiprocessQueue**: 프로세스 간 통신을 위한 `multiprocessing.Queue` 어댑터
  - `QueuePort` 인터페이스 준수
  - Async/Sync 통합 지원
- ✅ **TaskMetadata 확장**: `max_retries`, `fail_safe`, `stream_output`, `timeout` 필드 추가
- ✅ **Content-based DAG ID**: DAG 구조 기반 해시 ID 생성으로 재현성 보장

## 🎯 주요 기능

이 프로젝트는 5개의 핵심 시스템으로 구성되어 있습니다:

1. **Event System** 🔔 - 비동기 이벤트 메시징 시스템
2. **Scheduler System** ⏰ - 다양한 트리거 기반 스케줄링 시스템
3. **Task Registry** 📋 - Task 등록 및 메타데이터 관리 시스템
4. **Planner** 🗺️ - DAG 기반 실행 계획 시스템
5. **I/O Manager** 💾 - Task 결과 저장 및 로딩 시스템

### Event System
- 토픽 기반 pub/sub 패턴
- 와일드카드 패턴 매칭 (`*`, `**`)
- 비동기 제너레이터 기반 이벤트 스트림
- Hexagonal Architecture (포트 & 어댑터)
- **MultiprocessQueue**: `multiprocessing.Queue` 기반 프로세스 간 통신 (IPC) 지원
  - Async/Sync 통합 지원
  - 프로세스 안전한 Queue 구현

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
- **고급 Task 메타데이터**:
  - `max_retries`: Task 실패 시 재시도 횟수
  - `fail_safe`: 실패 시에도 파이프라인 계속 실행 여부
  - `stream_output`: Generator 기반 스트리밍 출력 지원
  - `timeout`: Task 실행 시간 제한 (초)

### Planner
- DAG(Directed Acyclic Graph) 자동 생성
- 순환 참조 감지 (Cycle Detection)
- 위상 정렬 (Topological Sort)
- 병렬 실행 레벨 계산
- 의존성 검증
- **Content-based DAG ID**: 동일한 구조의 DAG는 동일한 ID 생성 (해시 기반)
- **Schema Validator**: Task 간 타입 호환성 검증
  - Control Flow vs Data Flow Dependency 구분
  - Named Arguments (파라미터 이름 = task 이름)
  - Advanced Type Checking (Generic, Union, Inheritance)
  - Optional Parameters (기본값 있는 파라미터는 검증 제외)

### I/O Manager
- Hexagonal Architecture (포트 & 어댑터)
- Task 결과 저장/로딩 추상화
- **FilesystemIOManager**: 로컬 파일 시스템 기반 구현
  - Pickle 기반 직렬화
  - Run ID 기반 격리 (각 실행은 독립된 공간)
  - Task Result ID 지원 (스트리밍 출력의 개별 결과 저장)
  - 구조화된 디렉토리: `{base_path}/{run_id}/{task_name}/{task_result_id}.pkl`
- Run 단위 관리 (list, clear 지원)

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
│   │   │   ├── in_memory_queue.py
│   │   │   └── multiprocess_queue.py
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
│   ├── planner/                # Planner 시스템
│   │   ├── domain/            # 도메인 로직
│   │   │   ├── node.py        # Node 모델
│   │   │   ├── dag.py         # DAG 클래스
│   │   │   ├── dag_builder.py # DAG 생성
│   │   │   ├── execution_plan.py  # 실행 계획
│   │   │   ├── schema_validator.py  # Schema 검증
│   │   │   └── planner.py     # Planner 통합
│   │   └── README.md
│   │
│   ├── io_manager/             # I/O Manager 시스템
│   │   ├── domain/            # 포트 인터페이스
│   │   │   └── io_manager_port.py
│   │   └── infrastructure/    # 어댑터 구현
│   │       └── filesystem_io_manager.py
│   │
│   └── main.py                # 메인 진입점
│
├── examples/                   # 예제 코드
│   ├── schema_extraction_demo.py
│   ├── task_registry_example.py
│   ├── planner_example.py
│   └── schema_validator_example.py
│
├── tests/                      # 테스트
│   ├── event_system/
│   ├── scheduler/
│   ├── task_registry/
│   ├── planner/
│   ├── io_manager/
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

# Planner 예제
python -m examples.planner_example
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

### 4. Planner 사용하기

```python
from app.task_registry import task
from app.planner import get_planner

# Task 등록 (의존성 포함)
@task(name="extract", tags=["etl"])
def extract():
    return {"data": [1, 2, 3]}

@task(name="transform", tags=["etl"], dependencies=["extract"])
def transform():
    return [2, 4, 6]

@task(name="load", tags=["etl"], dependencies=["transform"])
def load():
    pass

# 실행 계획 생성
planner = get_planner()
plan = planner.create_execution_plan(tags=["etl"])

print(f"Execution order: {plan.execution_order}")
# Output: ['extract', 'transform', 'load']

print(f"Parallel levels: {plan.parallel_levels}")
# Output: [['extract'], ['transform'], ['load']]

# 실행 시뮬레이션
completed = set()
for level_tasks in plan.parallel_levels:
    # 이 레벨의 Task들은 병렬 실행 가능
    for task_id in level_tasks:
        print(f"Executing {task_id}...")
    completed.update(level_tasks)
```

### 5. I/O Manager 사용하기

```python
from app.io_manager import FilesystemIOManager
from pathlib import Path

# I/O Manager 초기화
io_manager = FilesystemIOManager(base_path=Path("/tmp/dp-poc-runs"))

# Run ID 생성 (보통 UUID 사용)
run_id = "run_20250115_001"

# Task 결과 저장
data = {"processed": [1, 2, 3, 4, 5], "count": 5}
path = io_manager.save(
    run_id=run_id,
    task_name="extract_data",
    task_result_id="result_001",
    value=data
)
print(f"Saved to: {path}")

# Task 결과 로딩
loaded_data = io_manager.load(
    run_id=run_id,
    task_name="extract_data",
    task_result_id="result_001"
)
print(f"Loaded: {loaded_data}")

# 스트리밍 출력 지원
for i, chunk in enumerate(data_generator()):
    io_manager.save(
        run_id=run_id,
        task_name="streaming_task",
        task_result_id=f"chunk_{i}",
        value=chunk
    )

# Run 내 모든 결과 조회
results = io_manager.list_results(run_id=run_id, task_name="streaming_task")
print(f"Found {len(results)} results")

# Run 정리
deleted_count = io_manager.clear_run(run_id=run_id)
print(f"Deleted {deleted_count} results")
```

### 6. 고급 Task 메타데이터 사용하기

```python
from app.task_registry import task

@task(
    name="resilient_task",
    max_retries=3,              # 실패 시 최대 3번 재시도
    fail_safe=True,             # 실패해도 파이프라인 계속 진행
    timeout=60,                 # 60초 타임아웃
    stream_output=False         # 단일 결과 반환
)
def resilient_task(data: dict) -> dict:
    # 네트워크 요청 등 실패 가능한 작업
    return process_data(data)

@task(
    name="streaming_task",
    stream_output=True,         # Generator 반환
    timeout=300                 # 5분 타임아웃
)
def streaming_task(batch_size: int) -> Generator[list, None, None]:
    # 대용량 데이터를 청크 단위로 처리
    for chunk in read_large_dataset(batch_size):
        yield process_chunk(chunk)
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
pytest tests/planner/ -v
pytest tests/io_manager/ -v

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
- [Planner](app/planner/README.md) - DAG 기반 실행 계획 시스템
- [I/O Manager](app/io_manager/) - Task 결과 저장/로딩 시스템

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
- [x] ~~Task DAG 실행 계획~~ (Planner 완료)
- [x] ~~I/O Manager 시스템~~ (FilesystemIOManager 완료)
- [x] ~~MultiprocessQueue 구현~~ (완료)
- [ ] Task DAG 실행 엔진 (Executor)
  - [ ] ExecutableTask 도메인 모델
  - [ ] Worker 프로세스 구현
  - [ ] Orchestrator 구현
  - [ ] MultiprocessExecutor 통합
- [ ] MongoDB I/O Manager 어댑터
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
