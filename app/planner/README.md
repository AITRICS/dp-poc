# Planner

Task 의존성을 분석하여 DAG(Directed Acyclic Graph)를 생성하고 실행 계획을 수립하는 시스템입니다.

## 특징

- ✅ **DAG 생성**: TaskRegistry의 Task들로부터 자동으로 DAG 구축
- ✅ **순환 참조 감지**: DFS 기반 순환 참조 자동 감지
- ✅ **위상 정렬**: Kahn's algorithm을 사용한 실행 순서 계산
- ✅ **병렬 레벨 계산**: 동시 실행 가능한 Task 그룹 자동 계산
- ✅ **의존성 검증**: 고아 노드, 자기 참조 등 다양한 검증
- ✅ **필터링 지원**: 태그나 Task 이름으로 필터링 가능

## 빠른 시작

### 1. Task 등록 및 실행 계획 생성

```python
from app.task_registry import task
from app.planner import get_planner

# Task 등록
@task(name="extract", tags=["etl"])
def extract() -> dict:
    return {"data": [1, 2, 3]}

@task(name="transform", tags=["etl"], dependencies=["extract"])
def transform(data: dict) -> list:
    return [x * 2 for x in data["data"]]

@task(name="load", tags=["etl"], dependencies=["transform"])
def load(data: list) -> None:
    print(f"Loaded: {data}")

# Planner로 실행 계획 생성
planner = get_planner()
plan = planner.create_execution_plan(tags=["etl"])

print(f"Execution order: {plan.execution_order}")
# Output: ['extract', 'transform', 'load']

print(f"Parallel levels: {plan.parallel_levels}")
# Output: [['extract'], ['transform'], ['load']]
```

### 2. 의존성 검증

```python
from app.planner import get_planner

planner = get_planner()

# 검증만 수행
errors = planner.validate_plan(tags=["etl"])

if errors:
    print("Validation errors:")
    for error in errors:
        print(f"  - {error}")
else:
    print("✓ All dependencies are valid!")
```

### 3. 병렬 실행 시뮬레이션

```python
from app.planner import get_planner

planner = get_planner()
plan = planner.create_execution_plan(tags=["etl"])

# 실행 시뮬레이션
completed = set()

for level_num, level_tasks in enumerate(plan.parallel_levels):
    print(f"Level {level_num}: {level_tasks}")

    # 이 레벨의 Task들은 병렬 실행 가능
    for task_id in level_tasks:
        node = plan.get_node(task_id)
        assert node.is_ready(completed)  # 모든 upstream 완료 확인
        print(f"  Executing {task_id}...")

    # 완료 표시
    completed.update(level_tasks)

# 다음 실행 가능한 Task 확인
ready_nodes = plan.get_ready_nodes(completed)
print(f"Next ready: {[n.node_id for n in ready_nodes]}")
```

## 주요 컴포넌트

### Node

DAG의 기본 단위로 하나의 Task를 표현합니다.

```python
from app.planner import Node

# Node는 TaskMetadata를 포함
node = Node(
    task=task_metadata,
    node_id="task_name",
    upstream={"dependency1", "dependency2"},
    downstream={"dependent1"}
)

# 노드 상태 확인
node.is_root()  # 의존성이 없는 노드
node.is_leaf()  # 자식이 없는 노드
node.is_ready(completed_set)  # 실행 가능 여부
```

### DAG

노드들의 관계를 관리하는 그래프 구조입니다.

```python
from app.planner import DAG

dag = DAG()
dag.add_node(node)

# 검증
errors = dag.validate()  # 순환 참조, 고아 노드 등 검증

# 위상 정렬
order = dag.topological_sort()  # Kahn's algorithm

# 레벨 계산
dag.calculate_levels()  # 병렬 레벨 계산

# 조회
root_nodes = dag.get_root_nodes()  # 루트 노드들
leaf_nodes = dag.get_leaf_nodes()  # 리프 노드들
```

### DAGBuilder

TaskRegistry로부터 DAG를 생성합니다.

```python
from app.planner import DAGBuilder
from app.task_registry import get_registry

builder = DAGBuilder(get_registry())

# 전체 Task로 DAG 생성
dag = builder.build_dag()

# 태그로 필터링
dag = builder.build_dag(tags=["etl"])

# 특정 Task만 선택
dag = builder.build_dag(task_names=["task_a", "task_b"])
```

### ExecutionPlan

실행 가능한 계획을 담은 객체입니다.

```python
from app.planner import ExecutionPlan

plan = ExecutionPlan(dag=dag)

# 실행 순서
print(plan.execution_order)  # ['A', 'B', 'C', ...]

# 병렬 레벨
print(plan.parallel_levels)  # [['A'], ['B', 'C'], ['D']]

# 다음 실행 가능한 노드
ready = plan.get_ready_nodes(completed={"A", "B"})

# 노드 레벨 확인
level = plan.get_level("task_name")
```

### Planner

전체 프로세스를 통합하는 메인 인터페이스입니다.

```python
from app.planner import DAGBuilder, Planner
from app.task_registry import get_registry

# DAG Builder 생성 후 Planner 생성
dag_builder = DAGBuilder(get_registry())
planner = Planner(dag_builder)

# 실행 계획 생성
plan = planner.create_execution_plan(
    task_names=None,  # None이면 전체
    tags=["etl"]      # 태그로 필터링
)

# 검증만 수행
errors = planner.validate_plan(tags=["etl"])

# Task 개수 확인
count = planner.get_task_count(tags=["etl"])
```

## 아키텍처

```
app/planner/
├── domain/
│   ├── node.py             # Node 모델
│   ├── dag.py              # DAG 클래스 (순환 감지 포함)
│   ├── dag_builder.py      # DAGBuilder
│   ├── execution_plan.py   # ExecutionPlan
│   └── planner.py          # Planner (통합)
└── __init__.py             # get_planner() export
```

## 알고리즘

### 순환 참조 감지 (Cycle Detection)

DFS 기반 White-Gray-Black 알고리즘 사용:

- **White**: 미방문 노드
- **Gray**: 방문 중인 노드 (현재 DFS 경로에 있음)
- **Black**: 방문 완료 노드

Gray 노드를 다시 방문하면 순환이 감지됩니다.

### 위상 정렬 (Topological Sort)

Kahn's Algorithm 사용:

1. In-degree가 0인 노드들을 큐에 추가
2. 큐에서 노드를 하나씩 꺼내서 결과에 추가
3. 해당 노드의 downstream들의 in-degree 감소
4. In-degree가 0이 되면 큐에 추가
5. 반복

### 병렬 레벨 계산

위상 정렬 순서대로 레벨 계산:

- Root 노드는 레벨 0
- 노드의 레벨 = max(upstream 노드들의 레벨) + 1
- 같은 레벨의 노드들은 병렬 실행 가능

## 사용 예제

### 단순 선형 DAG

```python
# A → B → C
@task(name="A")
def task_a(): pass

@task(name="B", dependencies=["A"])
def task_b(): pass

@task(name="C", dependencies=["B"])
def task_c(): pass

planner = get_planner()
plan = planner.create_execution_plan()

# 결과:
# execution_order: ['A', 'B', 'C']
# parallel_levels: [['A'], ['B'], ['C']]
```

### 병렬 실행 DAG

```python
# A → B, A → C (B와 C는 병렬)
@task(name="A")
def task_a(): pass

@task(name="B", dependencies=["A"])
def task_b(): pass

@task(name="C", dependencies=["A"])
def task_c(): pass

planner = get_planner()
plan = planner.create_execution_plan()

# 결과:
# execution_order: ['A', 'B', 'C'] (또는 ['A', 'C', 'B'])
# parallel_levels: [['A'], ['B', 'C']]  # B와 C는 동시 실행 가능
```

### 다이아몬드 구조

```python
# A → B → D, A → C → D
@task(name="A")
def task_a(): pass

@task(name="B", dependencies=["A"])
def task_b(): pass

@task(name="C", dependencies=["A"])
def task_c(): pass

@task(name="D", dependencies=["B", "C"])
def task_d(): pass

planner = get_planner()
plan = planner.create_execution_plan()

# 결과:
# parallel_levels: [['A'], ['B', 'C'], ['D']]
```

### 순환 참조 감지

```python
# A → B → C → A (에러!)
@task(name="A", dependencies=["C"])  # ❌ 순환 참조
def task_a(): pass

@task(name="B", dependencies=["A"])
def task_b(): pass

@task(name="C", dependencies=["B"])
def task_c(): pass

planner = get_planner()

# 검증 실패
errors = planner.validate_plan()
print(errors)  # ['Cycle detected: A -> B -> C -> A']
```

## 검증 항목

Planner는 다음 항목을 자동으로 검증합니다:

1. **순환 참조**: A → B → C → A 같은 순환 의존성
2. **고아 노드**: 존재하지 않는 Task에 대한 의존성
3. **자기 참조**: Task가 자기 자신에게 의존하는 경우

## 향후 계획

- [ ] **Critical Path 분석**: 가장 긴 실행 경로 계산
- [ ] **비용 최적화**: 예상 실행 시간 기반 최적화
- [ ] **조건부 실행**: 동적 실행 조건 지원
- [ ] **시각화**: Mermaid 다이어그램 생성
- [ ] **부분 실행**: 특정 Task만 실행 (upstream 포함)

## 관련 문서

- [Task Registry](../task_registry/README.md) - Task 등록 및 메타데이터 관리
- [Examples](../../examples/planner_example.py) - 전체 사용 예제
