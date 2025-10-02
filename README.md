# dp-poc

임시로 만들어서 진행하고 있는 data-platform 코드 입니다.

## 개발 환경 설정

### 필수 요구사항
- Python 3.12+
- [uv](https://github.com/astral-sh/uv)

### 설치

```bash
# 개발 환경 설정 (가상환경 생성 + 의존성 설치)
make dev

# 또는 의존성만 설치
make install
```

### Pre-commit 설정

```bash
# pre-commit 훅 설치
make pre-commit-install

# pre-commit 수동 실행
make pre-commit
```

## 개발 도구

### 코드 품질 도구

프로젝트는 다음 도구들을 사용합니다:

- **ruff**: Python linter 및 formatter
- **mypy**: 정적 타입 체커
- **isort**: import 정렬
- **pre-commit**: Git pre-commit 훅

### 사용 가능한 명령어

```bash
# 코드 품질
make lint          # 코드 린팅 (자동 수정)
make format        # 코드 포맷팅
make type-check    # 타입 체크
make pre-commit    # pre-commit 실행 (모든 파일)

# 테스트
make test          # 모든 테스트 실행
make test-cov      # 커버리지와 함께 테스트 실행
make test-unit     # 유닛 테스트만 실행
make test-integration  # 통합 테스트만 실행

# 실행
make run           # 애플리케이션 실행
```

## 프로젝트 구조

```
dp-poc/
├── app/
│   ├── event_system/         # 이벤트 시스템
│   │   ├── domain/          # 도메인 계층 (포트 정의)
│   │   └── infrastructure/  # 인프라 계층 (어댑터 구현)
│   └── main.py              # 애플리케이션 진입점
├── tests/                   # 테스트 코드
│   ├── conftest.py          # pytest 설정 및 fixtures
│   ├── test_broker.py       # Broker 테스트
│   ├── test_publisher.py    # Publisher 테스트
│   ├── test_consumer.py     # Consumer 테스트
│   └── test_integration.py  # 통합 테스트
├── pyproject.toml           # 프로젝트 설정 및 의존성
├── Makefile                 # 개발 명령어
└── .pre-commit-config.yaml  # pre-commit 설정
```

## 이벤트 시스템

프로젝트는 포트 & 어댑터 (Hexagonal Architecture) 패턴을 사용한 이벤트 시스템을 구현하고 있습니다:

- **도메인 계층**: 비즈니스 로직과 포트 인터페이스 정의
- **인프라 계층**: 실제 구현체 (In-Memory 브로커, 퍼블리셔, 컨슈머)
- **비동기 제너레이터**: 이벤트 스트림 처리를 위한 `AsyncGenerator` 패턴 사용
