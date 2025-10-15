# dp-poc

이벤트 기반 데이터 플랫폼의 핵심 컴포넌트를 구현한 PoC(Proof of Concept) 프로젝트입니다.


## 📊 Task의 전체 생명주기 (Life Cycle)

### 1단계: Task 정의 및 등록 📝

```
개발자가 코드 작성
─────────────────────────────────────────────
@task(name="extract_sales", tags=["etl"])
def extract_sales() -> pd.DataFrame:
    return pd.DataFrame({"sales": [100, 200]})

@task(name="transform", dependencies=["extract_sales"])
def transform(extract_sales: pd.DataFrame) -> pd.DataFrame:
    return extract_sales * 2

         ↓ decorator가 자동으로 처리

┌────────────────────────────────────────┐
│      TaskRegistry (전역 저장소)        │
├────────────────────────────────────────┤
│  "extract_sales" → TaskMetadata {      │
│    name: "extract_sales"               │
│    func: <function extract_sales>      │
│    dependencies: []                    │
│    tags: ["etl"]                       │
│  }                                     │
│                                        │
│  "transform" → TaskMetadata {          │
│    name: "transform"                   │
│    func: <function transform>          │
│    dependencies: ["extract_sales"]     │
│  }                                     │
└────────────────────────────────────────┘
```

### 2단계: DAG 빌드 🏗️

```
사용자가 실행 요청
─────────────────────────────────────────────
planner = Planner(DAGBuilder(registry))
execution_plan = planner.create_execution_plan(
    tags=["etl"]
)

         ↓

┌─────────────────────────────────────────┐
│          DAGBuilder                     │
│  TaskRegistry에서 Task들 가져옴         │
└─────────────────────────────────────────┘
         ↓

┌─────────────────────────────────────────┐
│              DAG                        │
│  (TaskMetadata들의 관계 그래프)         │
│                                         │
│     extract_sales (Node)                │
│         ↓                               │
│     transform (Node)                    │
│                                         │
│  각 Node가 TaskMetadata를 참조          │
└─────────────────────────────────────────┘
         ↓ DAGAnalyzer 실행

┌─────────────────────────────────────────┐
│         ExecutionPlan                   │
│  (실행 가능한 계획)                     │
│                                         │
│  execution_order: ["extract_sales",    │
│                    "transform"]         │
│  parallel_levels: [                    │
│    ["extract_sales"],  # Level 0       │
│    ["transform"]       # Level 1       │
│  ]                                      │
└─────────────────────────────────────────┘
```

### 3단계: 실행 준비 ⚙️

```
Orchestrator가 실행 시작
─────────────────────────────────────────────
orchestrator.start_pipeline(execution_plan)

         ↓

┌─────────────────────────────────────────┐
│       ExecutionContext                  │
│  (이 특정 실행의 컨텍스트)              │
│                                         │
│  run_id: "run_20251015_001"            │
│  dag_id: "etl_pipeline"                │
│  execution_plan: <위에서 만든 plan>     │
│  io_manager: <FilesystemIOManager>     │
│  metadata: {"user": "alice"}           │
└─────────────────────────────────────────┘
```

### 4단계: Task 실행 준비 🎯

```
Orchestrator가 개별 Task 실행 준비
─────────────────────────────────────────────

현재 상태:
  completed_tasks = {}  (아무것도 완료 안 됨)
  ready_tasks = ["extract_sales"]  (의존성 없음)

         ↓

Orchestrator._create_executable_task("extract_sales")

         ↓ ExecutionContext 사용
         ↓ TaskRegistry에서 TaskMetadata 조회
         ↓ upstream 확인 (없음)

┌─────────────────────────────────────────┐
│      ExecutableTask (1)                 │
│  (Worker가 실행할 패키지)               │
│                                         │
│  run_id: "run_20251015_001"            │
│  task_name: "extract_sales"            │
│  task_result_id: "result_001"          │
│  task_metadata: <TaskMetadata>         │
│    ├─ func: <function extract_sales>   │
│    ├─ dependencies: []                 │
│    └─ ...                              │
│  inputs: {}  (의존성 없음)              │
│  retry_count: 0                        │
└─────────────────────────────────────────┘
```

### 5단계: Worker 실행 ⚡

```
Worker가 ExecutableTask 받아서 실행
─────────────────────────────────────────────

┌─────────────────────────────────────────┐
│           Worker                        │
│                                         │
│  1. ExecutableTask 받음                 │
│     ↓                                   │
│  2. inputs 확인 (비어있음)              │
│     ↓                                   │
│  3. task_metadata.func 실행             │
│     result = extract_sales()            │
│     # pd.DataFrame({"sales": [100,200]})│
│     ↓                                   │
│  4. io_manager로 결과 저장              │
│     io_manager.save(                    │
│       "extract_sales",                  │
│       "run_20251015_001",               │
│       "result_001",                     │
│       result                            │
│     )                                   │
└─────────────────────────────────────────┘

         ↓ 파일시스템에 저장

📁 .dp-poc/runs/
  └─ run_20251015_001/
     └─ extract_sales/
        └─ result_001.pkl  ✅ 저장됨!
```

### 6단계: 다음 Task 실행 🔄

```
Orchestrator가 다음 Task 준비
─────────────────────────────────────────────

현재 상태:
  completed_tasks = {
    "extract_sales": "result_001"  ✅
  }
  ready_tasks = ["transform"]  (의존성 충족!)

         ↓

Orchestrator._create_executable_task("transform", completed_tasks)

         ↓ upstream 확인: ["extract_sales"]
         ↓ "extract_sales"의 결과 찾기
         ↓ io_manager.exists(...) → True

┌─────────────────────────────────────────┐
│      ExecutableTask (2)                 │
│                                         │
│  run_id: "run_20251015_001"            │
│  task_name: "transform"                │
│  task_result_id: "result_002"          │
│  task_metadata: <TaskMetadata>         │
│    ├─ func: <function transform>       │
│    ├─ dependencies: ["extract_sales"]  │
│    └─ ...                              │
│  inputs: {                             │  ← 중요!
│    "extract_sales": (                  │
│      "extract_sales",                  │
│      "result_001"                      │
│    )                                   │
│  }                                     │
│  retry_count: 0                        │
└─────────────────────────────────────────┘

         ↓ Worker로 전달

┌─────────────────────────────────────────┐
│           Worker                        │
│                                         │
│  1. ExecutableTask 받음                 │
│     ↓                                   │
│  2. inputs에서 "extract_sales" 발견     │
│     ↓                                   │
│  3. io_manager로 이전 결과 로드         │
│     extract_sales_data = io_manager.load(│
│       "extract_sales",                  │
│       "run_20251015_001",               │
│       "result_001"                      │
│     )                                   │
│     # pd.DataFrame({"sales": [100,200]})│
│     ↓                                   │
│  4. task_metadata.func 실행 (파라미터 주입)│
│     result = transform(                 │
│       extract_sales=extract_sales_data  │
│     )                                   │
│     # pd.DataFrame({"sales": [200,400]})│
│     ↓                                   │
│  5. io_manager로 결과 저장              │
│     io_manager.save(                    │
│       "transform",                      │
│       "run_20251015_001",               │
│       "result_002",                     │
│       result                            │
│     )                                   │
└─────────────────────────────────────────┘

         ↓

📁 .dp-poc/runs/
  └─ run_20251015_001/
     ├─ extract_sales/
     │  └─ result_001.pkl  ✅
     └─ transform/
        └─ result_002.pkl  ✅ 새로 저장됨!
```

## 🎯 전체 흐름 요약

```
┌─────────────────────────────────────────────────────────────┐
│                    Task의 전체 여정                          │
└─────────────────────────────────────────────────────────────┘

1️⃣ 정의 단계 (개발 시간)
   @task decorator
        ↓
   TaskMetadata 생성
        ↓
   TaskRegistry에 등록 ✅

2️⃣ 계획 단계 (실행 전)
   Planner.create_execution_plan()
        ↓
   DAGBuilder가 TaskRegistry에서 조회
        ↓
   DAG 생성 (Node들의 그래프)
        ↓
   DAGValidator 검증
        ↓
   DAGAnalyzer 분석 (순서, 레벨)
        ↓
   ExecutionPlan 생성 ✅

3️⃣ 실행 준비 단계 (런타임)
   Orchestrator.start_pipeline(execution_plan)
        ↓
   ExecutionContext 생성 (run_id 할당)
        ↓
   TaskDependencyResolver 생성 ✅

4️⃣ Task 생성 단계 (각 Task마다)
   Orchestrator._create_executable_task()
        ↓
   upstream 결과 찾기 (io_manager)
        ↓
   ExecutableTask 생성
        ↓
   inputs 매핑 추가 ✅

5️⃣ 실행 단계 (Worker)
   Worker.execute(executable_task)
        ↓
   inputs로부터 이전 결과 로드
        ↓
   task_metadata.func 실행
        ↓
   결과를 io_manager로 저장 ✅

6️⃣ 반복 (모든 Task 완료까지)
   completed_tasks 업데이트
        ↓
   get_ready_tasks() 호출
        ↓
   4️⃣ 단계로 돌아가기
        ↓
   모든 Task 완료 시 종료 ✅
```

## 🔑 핵심 개념 정리

```
┌────────────────────────────────────────────────────┐
│  TaskMetadata                                      │
│  = Task의 "정의"                                   │
│  (함수, 의존성, 설정 등)                           │
│  한 번 만들어지면 변하지 않음                       │
└────────────────────────────────────────────────────┘
                  ↓ 여러 개 모여서
┌────────────────────────────────────────────────────┐
│  DAG / ExecutionPlan                               │
│  = Task들의 "실행 계획"                            │
│  (순서, 의존성 관계)                               │
│  재사용 가능                                       │
└────────────────────────────────────────────────────┘
                  ↓ 실제 실행할 때
┌────────────────────────────────────────────────────┐
│  ExecutionContext                                  │
│  = 특정 실행의 "컨텍스트"                          │
│  (run_id, 어떤 plan, 어디에 저장)                  │
│  실행마다 새로 생성                                 │
└────────────────────────────────────────────────────┘
                  ↓ 각 Task마다
┌────────────────────────────────────────────────────┐
│  ExecutableTask                                    │
│  = Worker가 실행할 "패키지"                        │
│  (어떤 Task, 어떤 입력, 어디에 저장)                │
│  Task 실행마다 새로 생성                            │
└────────────────────────────────────────────────────┘
                  ↓ Worker가 실행
┌────────────────────────────────────────────────────┐
│  TaskResult                                        │
│  = 실행 결과                                       │
│  io_manager로 파일시스템에 저장                     │
└────────────────────────────────────────────────────┘
```

---


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

Nothing...

## 🤝 기여

내부 팀원들의 기여를 환영합니다. PR을 올리기 전에:

1. `make lint` 실행
2. `make test` 통과 확인
3. 새 기능은 테스트 코드 포함
4. 문서 업데이트
