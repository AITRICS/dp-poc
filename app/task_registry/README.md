# Task Registry

Task Registry 시스템은 함수를 Task로 등록하고 메타데이터를 관리하는 시스템입니다.

## 특징

- ✅ **동기/비동기 함수 지원**: `def`와 `async def` 모두 지원
- ✅ **의존성 관리**: Task 간 의존성 정의 및 검증
- ✅ **태그 기반 분류**: Task를 태그로 분류하고 조회
- ✅ **명시적 등록**: `@task` 데코레이터로 명시적 등록
- ✅ **자동 스키마 추출**: 타입 힌트에서 Input/Output 스키마 자동 추출
- ✅ **메타데이터 저장**: 함수 정보, 의존성, 설명 등 저장

## 빠른 시작

### 1. Task 등록

```python
from app.task_registry import task

# 동기 함수 - 스키마가 타입 힌트에서 자동으로 추출됩니다!
@task(name="extract_data", tags=["etl", "extract"])
def extract_data() -> dict[str, list[int]]:
    return {"data": [1, 2, 3, 4, 5]}

# 비동기 함수
@task(name="transform_data", tags=["etl", "transform"], dependencies=["extract_data"])
async def transform_data(data: dict[str, list[int]]) -> list[int]:
    return [x * 2 for x in data["data"]]

# 의존성이 있는 Task
@task(name="load_data", tags=["etl", "load"], dependencies=["transform_data"])
def load_data(data: list[int]) -> None:
    print(f"Loading {len(data)} items: {data}")
```

### 2. Registry 조회

```python
from app.task_registry import get_registry

registry = get_registry()

# Task 조회
task = registry.get("extract_data")
print(f"Task: {task.name}")
print(f"Dependencies: {task.dependencies}")
print(f"Is async: {task.is_async}")

# 모든 Task 조회
all_tasks = registry.get_all()
print(f"Total tasks: {len(all_tasks)}")

# 태그로 필터링
etl_tasks = registry.get_tasks_by_tag("etl")
print(f"ETL tasks: {[t.name for t in etl_tasks]}")

# 통계
stats = registry.get_stats()
print(f"Stats: {stats}")
```

### 3. 의존성 검증

```python
from app.task_registry import validate_all_tasks

# 모든 Task의 의존성 검증
errors = validate_all_tasks()

if errors:
    print("Validation errors:")
    for error in errors:
        print(f"  - {error}")
else:
    print("✅ All dependencies are valid!")
```

### 4. Task 메타데이터 조회

```python
from app.task_registry import get_registry

# Task 메타데이터 가져오기
registry = get_registry()
task_metadata = registry.get("transform_data")

if task_metadata:
    print(f"Task: {task_metadata.name}")
    print(f"Is Async: {task_metadata.is_async}")
    print(f"Dependencies: {task_metadata.dependencies}")
    print(f"Input Schema: {task_metadata.input_schema}")
    print(f"Output Schema: {task_metadata.output_schema}")
```

## Event System과 통합 (예정)

Task 완료 시 Event를 발행하여 다음 Task를 트리거할 수 있습니다:

```python
from app.task_registry import task
from app.event_system import EventBase

class TaskCompletedEvent(EventBase):
    def __init__(self, task_name: str, result: any, **kwargs):
        super().__init__(topic=f"task.completed.{task_name}", **kwargs)
        self.task_name = task_name
        self.result = result

@task(name="task_a")
async def task_a():
    result = {"data": 123}
    # Event 발행
    await publisher.publish(
        TaskCompletedEvent(task_name="task_a", result=result)
    )
    return result

# task_b는 EventTrigger로 대기
trigger = create_event_trigger(
    consumer=consumer,
    source_topic="task.completed.task_a",
    event_factory=lambda event: execute_task_b(event.result)
)
```

## 아키텍처

```
app/task_registry/
├── domain/                  # Domain 레이어
│   ├── task_model.py       # Task 메타데이터
│   └── registry_port.py    # Registry 인터페이스
│
├── infrastructure/          # Infrastructure 레이어
│   └── task_registry.py    # Registry 구현
│
├── utils/                   # Utilities
│   └── schema_utils.py     # 스키마 추출 유틸리티
│
└── decorator.py             # @task 데코레이터
```

## 자동 스키마 추출

함수의 타입 힌트에서 자동으로 input/output 스키마를 추출합니다:

```python
@task(name="my_task")
def my_task(x: int, y: str, config: dict[str, any]) -> list[int]:
    return [x] * len(y)

# 자동으로 추출됨:
# - input_schema: {'x': int, 'y': str, 'config': dict[str, any]}
# - output_schema: list[int]
```

타입 힌트가 없는 함수도 정상 작동:
```python
@task(name="no_hints")
def no_hints(a, b):
    return a + b

# input_schema: None
# output_schema: None
```

## 향후 계획

- [ ] **Planner**: DAG 생성 및 실행 순서 결정
- [ ] **Event 통합**: Task 완료 시 자동으로 Event 발행
- [ ] **DAG 시각화**: Task 의존성 그래프 시각화
- [ ] **병렬 실행**: 독립적인 Task들 병렬 실행
- [ ] **재시도 로직**: Task 실패 시 재시도
- [ ] **상태 관리**: Task 실행 상태 추적
