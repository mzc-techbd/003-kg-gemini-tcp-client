# TCP Latency Test Client (C++ / kqueue)

이 프로젝트는 지정된 TCP 서버에 연결하여 메시지를 보내고 응답 시간을 측정하는 C++ 기반의 TCP 클라이언트입니다. macOS/BSD 환경의 `kqueue`를 사용하여 비동기 I/O를 처리하며, 다수의 동시 연결을 효율적으로 관리합니다.

## 프로젝트 목표

*   다수의 TCP 연결을 동시에 생성하고 관리합니다.
*   지정된 서버로 미리 정의된 메시지(한국어 문장)를 전송합니다.
*   서버로부터의 응답 시간을 측정하여 지연 시간(latency) 통계를 계산합니다.
*   측정 결과를 파일에 저장합니다.

## 준비 과정

### 요구 사항

*   **운영체제:** macOS 또는 kqueue를 지원하는 BSD 계열 시스템
*   **컴파일러:** C++17 이상을 지원하는 C++ 컴파일러 (Clang, GCC 등)
*   **빌드 시스템:** CMake (버전 3.10 이상 권장)
*   **TCP 서버:** 클라이언트가 연결할 대상 TCP 서버가 실행 중이어야 합니다. 기본 설정은 `127.0.0.1:5001` 입니다. (필요시 `main.cpp` 수정)

### 입력 파일 준비

*   프로젝트 루트 디렉토리에 `korean_sentences.txt` 파일이 필요합니다. 이 파일에는 클라이언트가 서버로 전송할 한국어 문장들이 한 줄에 하나씩 포함되어야 합니다.
    ```
    안녕하세요
    반갑습니다
    테스트 메시지입니다
    ...
    ```
    빌드 시 이 파일은 빌드 디렉토리(`build/`)로 복사됩니다.

## 빌드 방법

1.  **빌드 디렉토리 생성 및 이동:**
    ```bash
    mkdir build
    cd build
    ```

2.  **CMake 실행:**
    ```bash
    cmake ..
    ```
    이 과정에서 필요한 `spdlog` 라이브러리를 자동으로 다운로드하고 설정합니다.

3.  **컴파일:**
    ```bash
    make
    ```
    빌드가 성공하면 `build` 디렉토리에 `tcp-client` 실행 파일이 생성됩니다.

## 실행 방법

1.  **TCP 서버 실행:** 클라이언트가 연결할 TCP 서버를 먼저 실행해야 합니다. 서버 주소와 포트는 `main.cpp`의 `SERVER_IP`와 `PORT` 상수에 정의되어 있습니다 (기본값: `127.0.0.1:5001`).

2.  **클라이언트 실행:**
    `build` 디렉토리에서 다음 명령어를 실행합니다.
    ```bash
    ./tcp-client
    ```
    클라이언트는 설정된 수만큼의 연결(`NUM_CONNECTIONS`)을 서버에 시도하고, 연결이 완료되면 `korean_sentences.txt` 파일의 문장들을 무작위로 서버에 보내기 시작합니다.

3.  **종료:**
    클라이언트 실행 중 터미널에 `exit`를 입력하고 Enter 키를 누르면 클라이언트가 정상적으로 종료됩니다.

## 설정

주요 설정값은 `main.cpp` 파일 상단에서 수정할 수 있습니다.

*   `NUM_CONNECTIONS`: 동시에 생성할 클라이언트 연결 수 (기본값: 200)
*   `SERVER_IP`: 대상 서버 IP 주소 (기본값: "127.0.0.1")
*   `PORT`: 대상 서버 포트 번호 (기본값: 5001)
*   `SENTENCE_FILE`: 입력 문장 파일 경로 (기본값: "korean_sentences.txt")
*   `RESULT_FILE`: 결과 저장 파일 경로 (기본값: "latency_results.txt")
*   `MAX_REQUESTS_BEFORE_SHUTDOWN`: 테스트를 위해 보낼 총 요청 수 (기본값: 10000)

## 출력

테스트가 완료되거나 `exit` 명령으로 종료되면, 측정된 지연 시간 통계가 `build` 디렉토리의 `latency_results.txt` 파일에 저장됩니다. 파일에는 평균, 최소, 최대, P99 지연 시간 등이 포함됩니다.

```
--- Test Results ---
Test Completed: Yes
Total Duration: 1.234 seconds
Target Requests: 10000
Total Responses Received: 10000
Total Latencies Measured: 10000
Connections with Data: 200/200
Average Latency: 5.678 ms
Minimum Latency: 1.234 ms
Maximum Latency: 15.987 ms
P99 Latency:     12.345 ms