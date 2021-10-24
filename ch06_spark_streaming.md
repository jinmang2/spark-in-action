# 6장 스파크 스트리밍으로 데이터를 흐르게 하자

스파크의 특장점
- 확장성
- 장애 내성
- 대규모 데이터 분석 기능 제공
- + 스트리밍 시스템 + 일괄 처리
    - 즉, Lambda Architecture 구축 가능
    - 실시간 레이어 + 배치 레이어

스파크 스트리밍
- 여러 파일 시스템(HDFS, AWS S3)과 다양한 분산 시스템(플럼, 카프카, 트위터) 등에서 데이터를 읽어 들일 수 있는 커넥터를 제공

## 6.1 스파크 스트리밍 애플리케이션 작성
스파크의 일괄 처리 기능을 실시간 데이터에 적용하는 방법은?
- 스파크 스트리밍의 **미니배치** 개념에서 찾을 수 있음
- 특정 시간 간격 내에 유입된 데이터 블록을 RDD로 구성

![image](https://user-images.githubusercontent.com/37775784/138548065-17fdad58-0bdb-4223-be1d-3750d2330a48.png)

위 그림처럼 다양한 외부 시스템 데이터를 스파크 스트리밍 잡으로 입수할 수 있음
- 외부 시스템? 단순한 파일 시스템이나 TCP/IP 접속 이외에도
- 카프카, 플럼, 트위터, 아마존 Kinesis 등을 의미

스파크 스트리밍은 각 데이터 소스별로 별도의 리시버를 제공
- 일부 데이터 소스는 리시버 없이도 데이터를 입수할 수 있음

각 리시버에는 해당 데이터 소스에 연결하고 데이터를 읽어 들여 스파크 스트리밍에 전달하는 로직이 구현되어 있음

스파크 스트리밍은 리시버가 읽어 들인 데이터를 미니배치 RDD로 (주기당 하나씩) 추가 분할함

스파크 애플리케이션은 이 미니배치 RDD를 애플리케이션에 구현된 로직에 따라 처리
- 위 로직에 ML이나 SQL같은 스파크 API의 모든 기능을 자유롭게 적용 가능

미니배치 처리 결과는 파일 시스템이나 관계형 데이터베이스 또는 다른 분산 시스템으로 내보낼 수 있음

### 6.1.1 예제 애플리케이션

```
요청: 대시보드 어플리케이션 구축
누가: 증권사
세부 사항:
    - 고객은 인터넷 애플리케이션을 사용해 거래 주문을 요청
        - 거래 주문? 유가 증권의 매매 등
    - 담당자는 고객의 주문을 받아 증권 시장에서 실제 거래를 진행
    - 초당 거래 건수
    - 누적 거래액이 가장 많은 고객 1~5위
    - 지난 1시간 동안 거래량이 가장 많았던 유가 증권 1~5위를 집계
```

본 장에선 HDFS 파일 데이터를 읽어 처리한 후 처리 결과를 다시 HDFS에 저장하는 단순한 예제 실습

본 장에선 초당 거래 건수 집계와 같은 단순한 예제부터 구현

모든 예제는 스파크 셸을 기준으로 설명
- 3장의 내용을 참고하여 이 예제를 스파크 독립형 애플리케이션으로 변경하고
- JAR 아카이브 형태로 클러스터에 제출할 수 있을 것

### 6.1.2 스트리밍 컨텍스트 생성

스파크 셸을 시작하고 예제를 실습. 가상 머신의 로컬 클러스터에서 실행 가능
- 어떤 클러스터를 사용하든 반드시 코어를 두 개 이상 실행자에 할당해야 함
- 스파크 스트리밍의 각 리시버가 입력 데이터 스트림을 처리하려면 코어(엄밀히 말하면 쓰레드(Thread))를 각각 한 개씩 사용해야 함
- 별도로 최소 코어 한 개 이상의 프로그램 연산을 수행하는데 필요함

로컬 클러스터 사용
- 아래의 명령으로 스파크 셸 시작

```shell
$ spark-shell --master local[4]
```

가장 먼저 `StreamingContext`의 인스턴스를 생성
- 스파크 셸에서 `SparkContext` 객체 (sc변수)와 `Duration` 객체를 사용해 `StreamingContext`를 초기화할 수 있음
- `Duration` 객체는 입력 데이터 스트림을 나누고 미니배치 RDD를 생성할 시간 간격을 지정하는 데 사용
- 최적의 미니배치 간격은 활용 사례, 성능 요구 사항, 클러스터 용량에 따라 다름
- 아래에선 5초로 설정
    - `Second` 대신 `Millisecond`와 `Minute` 객체를 사용해 주기 를 지정할 수도 있음

```scala
scala> import org.apache.spark._
scala> import org.apache.spark.streaming._
scala> val ssc = new StreamingContext(sc, Seconds(5))
```

아래처럼 스파크 설정 객체를 전달하면 `SparkStream`은 새로운 `SparkContext`를 시작

```scala
val conf = new SparkConf().setMaster("local[4]").setAppName("App name")
val ssc = new StreamingContext(conf, Seconds(5))
```

이 코드는 스파크 독립형 애플리케이션에서 사용할 수 있으나 스파크 셸에서는 `SparkContext` 두 개 이상을 동일 JVM에 초기화할 수 없기 때문에 동작하지 않는다.

### 6.1.3 이산 스트림 생성

#### 예제 데이터 내려받기

```
주문 시간: yyyy-MM-dd HH:mm:ss 형식
주문 ID: 순차적으로 증가시킨 정수
고객 ID: 1에서 100 사이 무작위 정수
주식 종목 코드: 80개의 주식 종목 코드 목록에서 무작위로 선택한 값
주문 수량: 1에서 1000 사이 무작위 정수
매매 가격: 1에서 100 사이 무작위 정수
주문 유형: 매수 주문(B) 또는 매도 주문(S)
```

터미널을 하나 더 열고 repo위치로 이동 후 아래 파일의 압축을 해제

```shell
$ cd first-edition/ch06
% tar xvzf orders.tar.gz
```

`StreamingContext`의 `textFileStream` 메서드를 사용해 파일의 텍스트 데이터를 스트리밍으로 직접 전달할 수 있음
- 위 메서드는 지정된 디렉터리를 모니터링하고
- 디렉터리에서 새로 생성된 파일을 개별적으로 읽음
- HDFS, 아마존 S3, GlusterFS, 로컬 디렉터리 등 하둡과 호환되는 모든 유형의 디렉터리를 지정할 수 있음
- 인수는 없음
- **새로 생성된** 파일을 읽는 메서드임
    - `StramingContext`를 시작할 시점에 이미 폴더에 있던 파일은 처리하지 않음
    - 파일에 데이터를 추가해도 읽지 않음
    - 즉, 스트리밍 처리를 시작한 시점 이후에 폴더로 복사된 파일들만 처리함

거래 주문 50만 건을 한꺼번에 시스템으로 유입하는 것은 다소 비현실적
- 즉, 간단한 리눅스 셸 스크립트(splitAndSend.sh)로 스트리밍 데이터 생성

```shell
#!/bin/bash

if [ -z "$1" ]; then
        echo "Missing output folder name"
        exit 1
fi

split -l 10000 --additional-suffix=.ordtmp orders.txt orders

for f in `ls *.ordtmp`; do
        if [ "$2" == "local" ]; then
                mv $f $1
        else
                hdfs dfs -copyFromLocal $f $1
                rm -f $f
        fi
        sleep 3
done
```

분할된 파일을 복사할 (HDFS 또는 로컬) 폴더를 골라서 입력 폴더로 설정

```scala
scala> val filestream = ssc.textFileStream("/home/spark/ch06input")
```

메서드의 결과로 `DStream` 클래스의 인스턴스를 반환
- Discrete Stream
- 스파크 스트리밍의 기본 추상화 객체
- 입력 데이터 스트림에서 주기적으로 생성하는 일련의 RDD 시퀀스를 표현
- `DStream`은 RDD와 마찬가지로 지연 평가
- 생성한 시점에서는 아무 일도 일어나지 않음
- 6.1.6절에서 스트리밍 컨텍스트를 시작하면 그때부터 RDD가 실제로 유입됨

### 6.1.4 이산 스트림 사용

앞에서는 `DStream` 객체를 생성했음. 이제 이 객체로 초당 거래 주문 건수를 계산해야 함

RDD와 마찬가지로 `DStream`은 이산 스트림을 다른 `DStream`으로 변환하는 다양한 메서드를 제공
- 이 메서드를 활용해서 `DStream`의 `RDD` 데이터에 필터링, 매핑, 리듀스 연산을 적용할 수 있고
- 다른 `DStream`과 결합하거나 조인이 가능하다

#### 데이터 파싱
먼저 파일의 각 줄을 다루기 쉬운 객체(e.g., 스칼라의 케이스 클래스)로 변환

```scala
scala> import java.sql.Timestamp
scala> case class Order(time: java.sql.Timestamp, orderId:Long, clientId:Long, symbol:String, amount:Int, price:Double, buy:Boolean)
```

- `Timestamp` 클래스는 스파크 `DataFrame`이 기본으로 지원하는 데이터 타입
- 즉, `DStream`에서 `DataFrame`을 만들 때도 이 `Order` 클래스를 바로 사용할 수 있음

다음으론 filestream DStream의 각 줄을 파싱하고 Order 객체로 구성된 `DStream`을 생성
- 다양한 방법으로 생성 가능
- 본 예제에서는 모든 RDD를 각 요소별로 처리하는 `flatMap` 변환 연산자 사용
- 포맷이 맞지 않는 데이터를 건너뛰기 위해 `map` 대신 위를 사용
- 전달한 매핑 함수는 데이터를 포맷대로 파싱할 수 있으면 해당 데이터를 리스트에 담아 반환하고
- 파싱에 실패하면 빈 리스트를 반환

```scala
import java.text.SimpleDataFormat
val orders = filestream.flatMap(line => {
    // 자바의 SimpleDataFormat을 활용해 타임스탬프 데이터를 파싱
    val dataFormat = new SimpleDataFormat("yyyy-MM-dd HH:mm:ss")
    val s = line.split(",") // 각 줄을 쉼표로 구분
    try {
        // 일곱 번째 필드에는 B(매수) 또는 S(매도)만 포함할 수 있음
        assert(s(6) == "B" || s(6) == "S")
        // 필드 값을 파싱해 Order 객체를 구성
        List(Order(new Timestamp(dataFormat.parse(s(0)).getTime()),
          s(1).toLong, s(2).toLong, s(3), s(4).toInt,
          s(5).toDouble, s(6) == "B"))
    }
    // 파싱 중 오류가 발생하면 해당 데이터와 오류 내용을 로그에 기록
    // 예제에서는 단순히 System.out에만 출력
    cache {
        case e : Throwable => println("Wrong line format ("+e+"): "+line)
        List() // 파싱을 완료할 수 없으면 빈 리스트를 반환해 해당 줄로 건너 뛴다.
    }
})
```

orders DStream의 각 RDD에는 Order 객체가 저장됨

#### 거래 주문 건수 집계

이제 초당 거래 주문 건수를 집계해보자! (본 챕터의 목적)
- 이 작업은 `PairDStreamFunctions`로 구현 가능
- 2-요소 튜플로 구성된 RDD는 `PairRDDFunctions` 인스턴스로 암시적 변환됨 (4장)
    - 여기서 잠깐! 암시적 변환이란?
        - https://programmingfbf7290.tistory.com/entry/21%EC%9E%A5-%EC%95%94%EC%8B%9C%EC%A0%81-%EB%B3%80%ED%99%98%EA%B3%BC-%EC%95%94%EC%8B%9C%EC%A0%81-%ED%8C%8C%EB%9D%BC%EB%AF%B8%ED%84%B0
        - 서로를 고려하지 않고 독립적으로 개발된 두 덩어리의 SW를 한데 묶을 때 유용
        - 동일한 어떤 대상을 각 라이브러리가 다르게 인코딩할 수 있음
        - 암시적 변환을 사용하면 한 타입을 다른 타입으로 변경하는 데 필요한 명시적인 변환의 숫자를 줄일 수 있음
        - 컴파일러가 타입 오류를 고치기 위해 규칙이 있다고 한다.
            - `implicit`으로 표시한 정의만 검토 대상
            - 삽입된 implicit 변환은 스코프 내에 단일 식별자로만 존재하거나 변환의 결과나 원래 타입과 연관이 있어야 한다.
        - x + y라는 표현식에서 타입 오류가 있다면 컴파일러는 convert(x) + 를 시도해본다고 함
            - 이 convert가 암시적 변환인 듯
        - `implicit def intToString(x: Int)=x.toString`
            - 컴파일러가 암시적 변환에 사용할 후보에 넣음
            - 타입 오류가 있을 때 체크하는 용도? 인듯
        - 타입 에러가 뜨면 나 못해~ 가 아니라 이렇게 하면 되지 않음? 하면서 직접 시도해보는 것!
        - 오이오이 컴파일러군 포기하지 말라구~
        - https://hamait.tistory.com/605
    - `PairDStreamFunctions`으로 변환된 객체는 `combineByKey`, `reduceByKey`, `flatMapValues` 및 여러 조인 함수를 포함한 변환 함수들을 제공

주문 유형별로 주문 건수를 집계하려면?
- 키와 값이 각각 주문 유형과 출현 횟수인 튜플을 사용해 orders DStream 요소를 매핑
- `reduceByKey`를 호출해 각 유형의 출현 횟수를 모두 더함
    - `countByKey` 함수는 없어서 못 쓴다고 함

```scala
scala> val numPerType = orders.map(o => (o.buy, 1L)).reduceByKey
```

위 코드는 튜플의 모든 값을 1로 초기화하고 동일 키의 값을 단순히 더했다.
- numPerType DStream의 각 RDD에는 (Boolean, Long) 튜플이 최대 두 개 저장된다고 한다.
- 하나는 매수 주문(true)의 건수고 나머지 하나는 매도 주문(false)의 건수라고 한다.
- 아니 1L은 뭘까? 이게 초기화의 코드로 보인다.
- key로 reduce함. 방법은 단순 합산!
- buy에서 가능한 값은 sell와 buy 최대 두 개임

### 6.1.5 결과를 파일로 저장

`saveAsTextFiles` 메서드를 사용해 앞서 계산한 결과를 파일로 저장!
- 인자: prefix string, suffix string
- `<prefix>-<millisecond>.<suffix>` 폴더에 저장
- prefix는 필수이며 suffix가 없을 시 `<prefix>-<millisecond>`에 저장됨.
    - 주기마다 저장한다는 소리
- 스파크 스트리밍은 디렉터리 아래에 미니배치 RDD의 각 파티션을 `part-xxxxx`라는 파일로 저장
    - `xxxxx`는 파티션의 이름
- RDD 폴더 별로 `part-xxxxx` 파일을 하나씩만 생성하려면 데이터를 저장하기 전에 DStream의 파티션을 한 개로 줄여야 한다.
- 위의 예제에서 `numPerType`의 각 RDD가 가진 요소의 개수는 최대 두 개임 (매도와 매수)
- 때문에 단일 파티션에 저장해도 메모리 문제가 발생하지 않음
    - 아닐 경우는 언제일까? 많을 때?

```scala
scala> numPerType.repartition(1).saveAsTextFiles("/home/spark/ch06output/output", "txt")
```

- 위 메서드는 출력 파일을 하둡과 호환되는 분산 파일 시스템(HDFS)에 저장
- 예제 프로그램을 로컬 클러스터에서 실행하면 로컬 파일 시스템에 저장됨.

```
Note.
DStream의 print(n) 메서드를 활용하면 스트리밍 애플리케이션을 간단하게 테스트할 수 있다.
이 메서드는 각 미니배치 RDD의 상위 n개(기본 값: 10) 요소를 출력한다.
```

- 아직 스트리밍 컨텍스트를 시작하지 않았기 때문에 아무런 변화가 없다.

### 6.1.6 스트리밍 계산 작업의 시작과 종료

드디어 노력의 열매를 수확할 때!

```scala
scala> ssc.start()
```

위 명령어로 스트리밍 컨텍스트를 시작한다.
- 스트리밍 컨텍스트는 지금까지 생성한 DStream들을 평가하고 리시버를 시작한 후 DStream 연산을 수행한다.
- 스파크 셸에서는 이 명령만 실행해도 애플리케이션의 스트리밍 처리를 시작하며 다른 명령은 필요하지 않다.

```
Note.
동일한 SparkContext 객체를 사용해 StreamingContext 인스턴스를 여러 개 생성할 수 있다.
하지만 동일 JVM에서는 StreamingContext를 한 번에 하나 이상 시작할 수 없다.
```

스파크 독립형 애플리케이션에서도 스파크 셸과 마찬가지로 `StreamingContext.start` 메서드를 호출해서 스트리밍 컨텍스트와 리시버를 시작한다. 하지만 아래 코드를 이어서 호출하지 않으면 리시버 스레드를 시작해도 드라이버의 메인 스레드가 종료된다.

```scala
ssc.awaitTermination()
```

- 위 메서드는 스파크 스트리밍의 계산 작업을 종료할 때까지 스파크 애플리케이션을 대기시킴

또는 `awaitTerminationOrTimeout` 메서드를 사용할 수도 있다고 한다.

#### 스파크 스트리밍으로 데이터 전송

위에서 실행은 했지만 아직 처리할 데이터가 없다. 앞서 언급한 [`splitAndSend.sh`](https://github.com/spark-in-action/first-edition/blob/master/ch06/splitAndSend.sh)를 사용해서 데이터를 밀어넣자.

아래 명령어로 데이터를 밀어넣기
- `orders.txt` 파일을 준비해야 정상적으로 동작
- local file system의 폴더를 사용할 때는 반드시 local 인자를 넣어줘야 함

```shell
$ chmod +x first-edition/ch06/splitAndSend.sh
$ mkdir /home/spark/ch06input
$ cd first-edition/ch06
$ ./splitAndSend.sh /home/spark/ch06input local
```

- 스크립트는 orders.txt 파일을 분할하고 지정된 폴더로 하나씩 복사
- 스트리밍 애플리케이션에서는 이 파일을 자동으로 읽어 주문 건수를 집계

#### 스파크 스트리밍 컨텍스트 종료

위 스크립트 실행 후 약 2분 30초 정도 지나면 모든 파일이 처리됨. 또는 아래 명령으로 종료 가능.

```scala
scala> ssc.stop(false)
```

- 전달하는 인자가 `false`이면 스파크 컨텍스트는 중지하지 않음
- 전달하지 않으면 `spark.streaming.stopSparkContextByDefault` 매개변수에 저장된 값이 대신 사용된다. (default: true)
- 종료된 스트리밍 컨텍스트는 다시 시작할 수 없다.
- 대신 스파크 컨텍스트를 재사용해 새로운 스트리밍 컨텍스트를 생성할 수 있다.
- 스파크 셸에서는 이전에 사용한 변수들을 덮어쓸 수 있기 때문에 입력한 코드를 다시 붙여 넣고 전체 애플리케이션을 재실행하는 것도 가능하다고 한다.

#### 출력 결과

`saveAsTextFiles` 메서드는 각 미니배치별로 새 폴더를 생성
- 하나를 열어보자
- `part-00000` 그리고 `_SUCCESS` 파일이 있다.
- `_SUCCESS`는 이 폴더의 쓰기 작업을 성공적으로 완료했음을 나타내는 파일
- 집계 결과는 `part-00000`에 기록됨

```
(false,9969)
(true,10031)
```

여러 파일에 저장된 데이터를 스파크 API를 사용하여 간편하게 불러올 수 있다.
- `SparkContext`의 `textFile`메서드에 파일 경로를 지정할 때 별표 문자(*)를 사용하면 스파크는 텍스트 파일 여러 개를 한꺼번에 읽어 들인다.

```scala
val allCounts = sc.textFile("/home/spark/ch06input/output*.txt")
```

### 6.1.7 시간에 따라 변화하는 계산 상태 저장

고객의 요구 사항 중 우리는 단 하나만을 수행했다. (스파크 스트리밍을 사용해서)

앞서 초당 거래 주문 건수를 집계할 때는 마지막 미니배치의 데이터만 사용했다. 그러나, 누적 거래액이 가장 많은 고객 1~5위와 지난 1시간 동안 거래량이 가장 많았던 유가 증권 1~5위를 계산하려면 이전 미니배치의 데이터도 함께 고려해야 한다.

아래 그림은 스파크 스트리밍이 상태를 유지하는 원리를 도식화한 것.

![image](https://user-images.githubusercontent.com/37775784/138561760-e98d4a63-8a45-4c97-9da7-529b029784f4.png)

자, 이걸 사용하면 시간에 따른 데이터 처리를 할 수 있다는 것을 알 수 있다. 어떤 메서드를 활용하면 될까?

#### updateStateByKey로 상태 유지

스파크 스트리밍은 윈도 연산 외에 아래 두 메서드를 제공
- `updateStateByKey`와 `mapWithState`
- 과거의 계산 상태를 현재 계산에 반영할 수 있는 방법!
- 두 메서드 모두 `PairDStreamFunctions` 클래스로 사용 가능
- 즉, key-value 튜플로 구성된 Pair DStream에서만 이 메서드를 사용할 수 있음

```
Note.
앞선 코드에서 스파크 스트리밍을 종료했었음. 때문에 아래 코드는 바로 실행하면 에러가 발생함
```

- 고객 ID를 key, 거래액을 value로 지정해 Pair DStream을 생성

```scala
val amountPerClient = orders.map(o => (o.clientId, o.amount * o.price))
```

두 가지 버전이 있다고 함
- DStream의 값만 처리
- DStream의 키와 값을 한꺼번에 처리, 키를 변경할 수도 있음

두 버전 모두 각 키의 상태값을 포함한 새로운 State DStream을 반환

예제에선 첫 번째 예제(값만 처리)를 다룸.

```
// updateStateByKey 메서드에 전달할 함수 시그니처
(Seq[V], Option[S]) => Option[S]
```

- 첫 번째 인자는 현재 미니배치에 유입된 각 키의 Seq 객체가 전달됨
- 두 번째 인자는 key의 상태 값이 option으로 전달됨
    - 해당 키의 상태를 아직 계산한 적이 없으면 Option 객체는 None을 반환
- 어떤 경우든 위 함수는 각 키의 새로운 상태 값을 반환해야 함.
- 메서드에 결과 DStream에 사용할 파티션의 개수나 Partioner 객체를 선택 인수로 지정할 수 있음
    - **대량의 키와 상태 객체를 유지할 땐 적절한 파티셔닝이 중요**

아래 코드는 `amountPerClient` DStream에 `updateStateByKey` 메서드를 적용해 State DStream을 생성
- https://github.com/spark-in-action/first-edition/blob/master/ch06/scala/ch06-listings.scala#L64

```scala
val amountState = amountPerClient.updateStateByKey((vals, totalOpt:Option[Double]) => {
  totalOpt match {
    // 이 키의 상태가 존재할 경우 상태 값에 새로 유입된 값의 합계를 더함
    case Some(total) => Some(vals.sum + total)
    // 이전 상태 값이 없을 경우 새로 유입된 값의 합계만 반환
    case None => Some(vals.sum)
  }
})
```

`amountState DStream`에서 개래액 1~5위 고객을 계산하려면 먼저 DStream의 각 RDD를 정렬한 후 각 RDD에서 상위 다섯 개 요소만 남겨야 함. 아래 코드로 수행
- `zipWithIndex`를 사용해서 RDD의 각 요소에 번호를 부여
- 상위 다섯 개 번호를 제외한 모든 요소를 필터링
- map 메서드로 번호를 제거

```scala
val top5clients = amountState.transform(_.sortBy(_._2, false).map(_._1).
  zipWithIndex.filter(x => x._2 < 5))
```

#### union으로 두 DStream 병합
지금까지 두 가지를 계산했다.
- 초당 거래 주문 건수
- 거래액 1~5위 고객

위 결과를 배치 간격당 한 번씩만 저장하려면 두 결과 DStream을 단일 DStream으로 병합해야 한다. `join`, `cogroup`, `union`을 활용하여 병합할 수 있고 예제에선 `union`을 사용

`union`을 사용해서 병합하려면 요소 타입이 서로 동일해야 함
- `top5clients` DStream과 `numPerType` DStream의 요소를 각각 2-요소 튜플로 통합

```scala
// 요소 타입 통일을 위해 두 번째 요소를 List로
val buySellList = numPerType.map(t =>
    if(t._1) ("BUYS", List(t._2.toString))
    else ("SELLS", List(t._2.toString)) )
// 모든 데이터를 단일 파티션으로 모음
val top5clList = top5clients.repartition(1).
    // 고객 ID만 남김
    map(x => x._1.toString).
    // 모든 고객 ID를 단일 배열로 모음
    glom().
    // 지표 이름을 키로 추가
    map(arr => ("TOP5CLIENTS", arr.toList))
// 요소 타입을 맞춰준 후 union으로 DStream 병합
val finalStream = buySellList.union(top5clList)
// 단일 파티션으로 모으고 파일에 저장
finalStream.repartition(1).saveAsTextFiles("/home/spark/ch06output/output", "txt")
```

위의 `glom` 변환 연산자는 4장에서 나온 것!
- RDD를 손쉽게 단일 배열로 만들 수 있음
- 물론 데이터가 충분히 작아햐 함...    

#### 체크포인트 디렉터리 지정

아래 코드로 체크포인트 디렉터리 지정

```scala
sc.setCheckpointDir("/home/spark/checkpoint/")
```

- 4장에 설명한 바와 같이 RDD 데이터와 전체 DAG(RD의 계산 계획)를 체크포인트로 지정 가능
- 일부 실행자를 비정상적으로 종료해도 RDD를 처음부터 다시 계산할 필요 없이 디스크에 저장된 데이터를 바로 읽어 들일 수 있음
- `updateStateByKey` 메서드를 사용할 경우 메서드가 반환하는 State DStream에 체크포인팅을 반드시 적용해야 함
    - 각 미니배치마다 RDD의 DAG를 계속 연장하면서 스택 오버플로 오류를 유발함
    - RDD를 주기적으로 체크포인트에 저장하면 RDD의 DAG가 과거의 미니배치에 의존하지 않도록 만들 수 있음

#### 두 번째 출력 결과

요구 사항 두 개를 만족시켰다!

```
(SELLS,List(4926))
(BUYS,List(5074))
(TOP5CLIENTS,List(34, 69, 92, 36, 64))
```

#### mapWithState
앞서 두 메서드가 있다고 언급했다. 이 메서드는 `updateStateByKey`의 기능 및 성능을 상당히 개선했다.
- 스파크 1.6부터 사용 가능

가장 큰 차이점?
- 상태 값의 타입과 반환 값의 타입을 다르게 적용할 수 있음

`mapWithState` 메서드는 `StateSpec` 클래스의 인스턴스만 인수로 받음
- `StateSpec` 객체를 초기화하려면 `StateSpec.function` 메서드에 다음 시그니처의 매핑 함수를 정의해서 전달해야 함
- 첫 번째 인수인 Time 인수는 선택 인수이므로 함수 정의에서 제외할 수 있음

```
// StateSpec.function 메서드에 전달할 함수 시그니처
(Time, KeyType, Option[ValueType], State[StateType]) => Option[MappedType]
```

Key, Value, State를 전달! 결과 DStream의 요소와 상태 값이 동일한 `updateStateByKey`와 다르게 상태값과 다른 `MappedType`을 반환.

`State` 인수는 각 키의 상태 값을 저장하는 객체
- `exists`: 상태 값이 존재하면 true를 반환
- `get`: 상태 값을 가져옴
- `remove`: 해당 키의 상태를 제거
- `update`: 해당 키의 상태 값을 갱신(또는 초기화)

```scala
//mapWithState
val updateAmountState = (time:Time, clientId:Long, amount:Option[Double], state:State[Double]) => {
    // 새로 유입된 값을 새로운 상태값으로 설정
    // 유입된 값이 없으면 0을 설정
    var total = amount.getOrElse(0.toDouble)
    if(state.exists())
        // 새로운 상태값에 기존 상태 값을 더함
        total += state.get()
    // 새로운 상태 값으로 상태를 갱신
    state.update(total)
    // 고객 ID와 새 상태 값을 2-요소 튜플로 만들어 반환
    Some((clientId, total))
}
// 앞의 amountState를 만드는 코드까지 재실행 후
// 위 매핑 함수를 mapWithState에 적용
val amountState = amountPerClient.mapWithState(StateSpec.function(updateAmountState)).stateSnapshots()
```

- 코드 마지막에 `stateSnapshots`를 호출함
- 만일 호출하지  않으면 위 메서드의 결과는 현재 미니배치의 주기 사이에 거래를 주문했던 고객 ID와 누적 거래액만 포함
- `stateSnapshots`은 전체 상태를 포함한 `DStream`을 반환

`StateSpec` 객체에는 매핑 함수 외에도 파티션 개수, Partitioner 객체, 초기 상태 값을 담은 RDD, 제한 시간 등을 설정할 수 있음
- 초기 상태 값을 지정하는 기능을 잘 활용하면 종료된 스트리밍 작업을 재시작할 때도 종료 전 상태 값을 유지하고 재사용 가능
- e.g., 하루 주식 시장을 마감할 때 고객 목록과 누적 거래액을 저장해 두면 다음 날에는 전날의 마지막 상태부터 다시 시작해 거래액 누적이 가능함

제한 시간을 설정하는 기능도 꽤 쓸만함
- 시간을 초과해 만료(expire)된 특정 상태 값을 삭제
- e.g., 고객이 접속한 세션의 만료 여부를 계산하는 데 이 기능 사용 가능
- `updateStateByKey`로 구현하려면 매핑 함수에서 만료 여부를 직접 계산해야 함 (왜?)

성능 면에서도 `mapWithState`가 유리.
- 키 별 상태를 x10배 유지 가능
- 처리 속도는 x6배 빠름
- 새로 유입된 데이터가 없는 키를 연산에서 제외하는 아이디어가 큰 몫을 함

### 6.1.8 윈도 연산으로 일정 시간 동안 유입된 데이터만 계산

마지막으로 해야할 업무는? 지난 1시간 동안 거래량이 가장 많았던 유가 증권 1~5위를 찾는 것
- 이 유형은 일정 시간 내의 데이터만 대상으로 계산해야 함
- 앞의 유형과는 다른 연산이 필요

위를 스파크 스트리밍에서 **윈도 연산(window operation)** 을 통해 구현

아래 크림은 윈도 연산의 원리를 도식화
- 미니배치의 슬라이딩 윈도를 기반으로 수행됨
- 스파크 스트리밍은 sliding window의 길이와 이동 거리를 바탕으로 윈도 DStream을 생성
- 슬라이딩 윈도의 길이과 이동 거리는 반드시 미니배치 주기의 배수여야 함.

![image](https://user-images.githubusercontent.com/37775784/138582619-c226ca5c-6eb6-4fac-b39e-5e6c1fc749ee.png)

#### 윈도 연산을 사용해 마지막 지표 계산

우리의 목적은 지난 1시간 동안 거래된 각 유가 증권별 거래량을 집계하는 것.
- 따라서 윈도의 길이 또한 1시간이 되야 함
- 이동 거리는 미니배치 주기(5초)와 동일하게 설정

```scala
// reduceByKeyAndWindow 메서드로 윈도 DStream을 생성하고 데이터를 리듀스 함수에 전달
val stocksPerWindow = orders.map(x => (x.symbol, x.amount)).window(Minutes(60)).
  reduceByKey((a1:Int, a2:Int) => a1+a2)

val topStocks = stocksPerWindow.transform(_.sortBy(_._2, false).map(_._1).
  zipWithIndex.filter(x => x._2 < 5)).repartition(1).
    map(x => x._1.toString).glom().
    map(arr => ("TOP5STOCKS", arr.toList))

// final DStream에 최종 결과를 추가
val finalStream = buySellList.union(top5clList).union(topStocks)

// 이전과 동일하게 저장하고 체크포인트 설정
finalStream.repartition(1).saveAsTextFiles("/home/spark/ch06output/output", "txt")
sc.setCheckpointDir("/home/spark/checkpoint/")
ssc.start()
```

최종 결과는 아래와 같다.

```
(SELLS,List(9969))
(BUYS,List(10031))
(TOP5CLIENT,List(15, 64, 55, 69, 19))
(TOP5STOCKS,List(AMD, INTC, BP, EGO, NEM))
```

#### 그 외 다른 윈도 연산자

|                                             연산자                                            |                                                                                                                                                                                    설명                                                                                                                                                                                    |
|---------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `window(winDur, [slideDur]) `                                                                   | 길이가 winDur인 슬라이딩 윈도 내 유입된 DStream 요소들을 slideDur 주기마다 RDD 형태로 생성한다. slideDur를 지정하지 않으면 미니배치 주기를 기본 값으로 사용한다.                                                                                                                                                                                                           |
| `countByWindow(winDur, slideDur)`                                                               | 길이가 winDur인 슬라이딩 윈도 내 유입된 DStream 요소들을 slideDur 주기마다 모아서 개수를 집계하고, 그 결과를 RDD에 담아 생성한다.                                                                                                                                                                                                                                          |
| `countByValueAndWindow(winDur, slideDur, [numParts])`                                           | 길이가 winDur이고 이동 거리가 slideDur인 슬라이딩 윈도 내 유입된 요소의 고윳값별로 개수를 집계한다. numParts로 결과 RDD에 적용할 파티션 개수를 변경할 수 있다.                                                                                                                                                                                                             |
| `reduceByWindow(reduceFunc, winDur, slideDur)`                                                  | 길이가 winDur인 슬라이딩 윈도 내 유입된 DStream 요소들을 slideDur 주기마다 reduceFunc 함수에 전달해 단일 값으로 리듀스하고, 그 결과를 RDD에 담아 생성한다.                                                                                                                                                                                                                 |
| `reduceByWindow(reduceFunc, invReduceFunc, winDur, slideDur)`                                   | 길이가 winDur인 슬라이딩 윈도에 새로 유입된 DStream 요소들을 slideDur 주기마다 reduceFunc 함수에 전달해 단일 값으로 리듀스한다. 이와 동시에 슬라이딩 윈도를 벗어난 기존 요소들을 역리듀스(invReduceFunc) 함수로 전달해 reduceFunc에서 리듀스한 단일 값에서 제외한 결과를 RDD에 담아 생성한다.3 이 메서드는 역리듀스 함수를 사용하지 않는 reduceByWindow보다 더 효율적이다. |
| `groupByKeyAndWindow(winDur, [slideDur], [numParts/partitioner])`                               | 길이가 winDur이고 이동 거리가 slideDur인 슬라이딩 윈도 내 유입된 요소들을 키별로 그루핑한다. 결과 RDD에 적용할 파티션 개수 또는 Partitioner를 선택 인수로 지정할 수 있다.                                                                                                                                                                                                  |
| `reduceByKeyAndWindow(reduceFunc, winDur, [slideDur], [numParts/partitioner])`                  | 길이가 winDur이고 이동 거리가 slideDur인 슬라이딩 윈도 내 유입된 요소들을 키별로 리듀스한다. 결과 RDD에 적용할 파티션 개수 또는 Partitioner를 선택 인수로 지정할 수 있다.                                                                                                                                                                                                  |
| `reduceByKeyAndWindow(reduceFunc, invReduceFunc, winDur, [slideDur], [numParts], [filterFunc])` | 리듀스 함수만 전달하는 reduceByKeyAndWindow와 기능은 동일하지만, 역리듀스 함수를 사용해 슬라이딩 윈도를 벗어난 요소들을 더 효율적으로 제외할 수 있다. 선택 인수인 filterFunc 함수로 결과 DStream에 남을 키-값 쌍의 조건을 지정할 수 있다.                                                                                                                                  |

### 6.1.9 그 외 내장 입력 스트림

#### 파일 입력 스트림

파일의 데이터는 `binaryRecordsStream` 메서드나 `fileStream` 메서드로 읽을 수 있다.
- `binaryRecordsStream`는 이진 파일에서 특정 크기의 레코드를 읽어서 바이트 배열(`Array[Byte]`)로 구성된 DStream을 반환
- `fileStream`은 타입 매개변수로 키의 클래스 타입과 같은 클래스 타입, HDFS 파일을 읽는 데 사용할 입력 포맷의 클래스 타입(하둡의 `NewInputFormat` 클래스를 상속한 하위 클래스)를 ㅣㅈ정해야 함

`fileStream` 메서드에는 타입 매개변수 외에 파일을 읽을 폴더 경로를 전달해야 하고 아래 선택 인수들을 추가 지정할 수 있다.
- `filter` 함수: 각 Path 객체(하둡에서 파일을 표현하는 클래스) 별로 해당 파일의 처리 여부를 판단
- `newFileOnly` 플래그: 모니터링 폴더 아래에 새로 생성된 파일만 처리할지 아니면 폴더의 모든 파일을 처리할지 지정
- `하둡 Configuration 객체` HDFS 파일을 읽는 데 필요한 추가 옵션들을 설정

#### 소켓 입력 스트림

스파크 스트리밍을 사용해서 TCP/IP 소켓에서 바로 데이터를 수신할 수도 있음

소켓 데이터는 `socketStream`, `socketTextStream` 메서드로 읽어 올 수 있음
- `socketTextStream`은 요속 텍스트, UTF-8로 인코딩된 문자열 줄로 구성된 DStream을 반환
    - 소켓이 연결한 호스트네임과 포트 번호를 전달해야 함
    - `StorageLevel`을 선택 인자로 지정. 기본값은 `StorageLevel.MEMORY_AND_DISK_SER_2`
    - 복제 계수를 2로 설정하고 RDD를 메모리와 디스크에 저장
- `socketStream` 메서드에도 위와 동일한 목록의 인자를 지정해야 하고 변환 함수를 추가로 전달해야 함
    - 변환 함수는 이진 데이터를 읽는 자바 `InputStream` 객체를 결과 DStream의 요소가 될 객체로 변환


## 6.2 외부 데이터 소스 사용

외부와 연결!

- [**카프카**](https://kafka.apache.org): 빠른 성능과 확장성을 갖춘 분산 발행-구독(publish-subscribe) 메시징 시스템. 모든 메시지를 유지하며 유실된 데이터를 재전송할 수 있음
- [**플럼**](https://flume.apache.org): 대량의 로그 데이터를 안정적으로 수집, 집계, 전송할 수 있는 분산 시스템
- [**아마존 Kinesis**](https://aws.amazon.com/en/kinesis): 카프카와 유사한 아마존 웹 서비스의 스트리밍 플랫폼
- [**트위터**](https://dev.twitter.com/overview/documentation): 인기 소셜 네트워크인 트위터가 제공하는 데이터 API
- [**ZeroMQ**](http://zeromq.org): 분산 메시징 시스템
- [**MQTT**](http://mqtt.org): 경량 발행-구독 메시징 프로토콜

### 6.2.1 카프카 시작

- 카프카 설치
- 카프카는 아파치 주키퍼를 사용
    - 주키퍼는 분산 프로세스를 안정적으로 조율할 수 있는 오픈소스 서버 소프트웨어
- 2181 포트로 주키퍼가 백그라운드에서 동작
- 거래 주문 데이터를 전송할 토픽(orders)과 지표 데이터를 전송할 토픽(metrics)을 생성
- 예제 코드 실습

### 6.2.2 카프카를 사용해 스트리밍 애플리케이션 개발

- 스파크 셸을 종료
- 카프카 라이브러리와 스파크-카프카 케넥터 라이브러리를 스카프 셸의 클래스패스에 추가해 재시작해야 함
- 각 JAR 파일을 직접 내려받을 수 있지만 아래처럼 packages 매개변수를 지정하는 자동으로 내려받음

```shell
$ spark-shell --master local[4] --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0,org.apache.kafka:kafka_2.11.0.8.2.1
```

- 위 packages 매개변수에서 각 라이브러리의 groupID, artifactID 및 라이브러리 버전 정보를 콜론(:)으로 구분해 나열

스파크 독립형 애플리케이션을 메이븐 프로젝트로 빌드할 때는 `pom.xml` 파일에 아래 의존 라이브러리를 추가

```
<dependency>
    <groupID>org.apache.spark</groupID>
    <artifactID>spark-streaming-kafka-0.8_2.11</artifactID>
    <version>2.0.0</version>
</dependency>
<dependency>
    <groupID>org.apache.kafka</groupID>
    <artifactID>kafka_2.11</artifactID>
    <version>0.8.2.1</version>
</dependency>
```

#### 스파크-카프카 커넥터 사용

스파크가 제공하는 카프카 케넥터에는
- 리시버 기반 커넥터(receiver-based connector)
    - 간혹 메시지 한 개를 여러 번 받아옴
    - 연산 성능이 다소 뒤떨어짐
    - 데이터 유실을 방지하려고 로그 선행 기입(write-ahead logging) 기법을 사용하기 때문
- 다이렉트 커넥터(direct connector)
    - 입력된 메시지를 정확히 한 번 처리할 수 있음


#### 카프카로 메시지 전송

6.1절에서 지표 계산 결과를 담은 `finalStream DStream`을 파일에 저장했었음. 이 결과를 파일 대신 카프카 토픽으로 결과를 전송
- 스파크 스트리밍에서 `foreachRDD` 기능으로 구현 가능

```
// foreachRDD 메서드 시그니처
def foreachRDD(foreachFunc: RDD[T] => Unit): Unit
def foreachRDD(foreachFunc: (RDD[T], Time) => Unit): Unit
```

카프카에 메시지를 전송하려면 카프카의 `Producer` 객체를 사용해야 함
- 카프카 브로커에 접속하고 `KeyedMessage` 객체의 형태로 구성한 메시지를 카프카 토픽으로 전송

카프카 `Producer` 객체는 직렬화할 수 없다는 제약이 있음
- 다른 JVM에서 계속 사용하는 것을 불가능
- 반드시 실행자에서 구동될 코드에 구현

책에서 제안하는 해결 방안 세 가지
- foreach 메서드 내에서 Producer 객체를 생성
    - 위 코드는 메시지를 하나 보낼 때마다 새로운 Producer 객체를 생성
- foreachPartition 메서드를 사용해 RDD 파티션별로 Producer를 하나씩 생성할 수 있음
    - 이 또한 최적의 방안은 아님
- 싱글톤(singleton) 객체를 생성해 JVM별로 Producer 객체를 하나씩만 초기화함

```scala
//code for kafkaProducerWrapper.jar
import kafka.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
case class KafkaProducerWrapper(brokerList: String) {
  val producerProps = {
    val prop = new java.util.Properties
    prop.put("metadata.broker.list", brokerList)
    prop
  }
  val p = new Producer[Array[Byte], Array[Byte]](new ProducerConfig(producerProps))

  def send(topic: String, key: String, value: String) {
    p.send(new KeyedMessage(topic, key.toCharArray.map(_.toByte), value.toCharArray.map(_.toByte)))
  }
}
object KafkaProducerWrapper {
  var brokerList = ""
  lazy val instance = new KafkaProducerWrapper(brokerList)
}
```

- 스파크 셸에서 이 객체를 드라이버에서 초기화하고 직렬화하지 않도록 JAR 파일로 컴파일해서 사용
- --jars 매개변수를 사용해서 JAR 파일을 스파크 셸의 클래스패스에 추가

```shell
$ spark-shell /
    --master local[4] /
    --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0,org.apache.kafka:kafka_2.11.0.8.2.1/
    --jars first-edition/ch06/kafkaProducerWrapper.jar
```

지금까지 학습한 모든 예제 코드를 결합해서 실행하면 외부로 로그를 보내는 것이 가능!

## 6.3 스파크 스트리밍의 잡 성능

스트리밍 애플리케이션이 갖춰야 할 비기능적 요구사항
- **낮은 지연 시간**: 각 입력 레코드를 최대한 빨리 처리
- **확장성**: 실시간 데이터의 유량 증가에 뒤처지지 않는다
- **장애 내성**: 일부 노드에 장애가 발생해도 유실 없이 계속 데이터를 입수한다

### 6.3.1 성능 개선

우선 미니배치의 주시를 결정
- 적절한 주기는 잡이 수행하는 연산 유형과 클러스터의 처리 용량에 따라 다름
- 만능 법칙은 없으나 각 스파크 애플리케이션을 실행할 때 자동으로 시작하는 스파크 웹 UI의 Streaming 페이지에서 활용할 수 있음
- 기본 포트 4040번으로 접속

Streaming 페이지는 다음 네 가지 지표를 시계열 그래프 형태로 제공
- **Input Rate(유입 속도)**: 초당 유입된 레코드 개수를 보여줌
- **Scheduling Delay(스케줄링 지연 시간)**: 새로운 미니배치의 잡을 스케줄링할 때까지 걸린 시간
- **Processing Time(처리 시간)**: 각 미니배치의 잡을 처리하는 데 걸린 시간
- **Total Delay(총 지연 시간)**: 각 미니배치를 처리하는 데 소요된 총 시간

미니배치 당 총 처리 시간 (즉, 총 지연 시간)은 미니배치 주기보다 짧아야 하고 일정한 값으로 유지해야 함

반면 총 처리 시간이 계속 증가하면 스트리밍 연산을 장기간 지속할 수 없다.

![image](https://user-images.githubusercontent.com/37775784/138583258-e9650346-2f3c-404b-924b-4ac211453c55.png)

#### 처리 시간 단축

스트리밍 프로그램의 최적화를 시도해 배치당 처리 시간을 최대한 단축시켜야 함
- 4장의 설명처럼 불필요한 셔플링은 최대한 피하자
- 카프카 예제처럼 파티션 내에서 커넥션을 재사용하고 커넥션 풀링(connection pooling) 등 기법을 활용

미니배치 주기를 더 길게 늘릴 수 있음
- 미니배치 주기가 늘어나면 잡 스케줄링, 태스크 직렬화, 데이터 셔플링 등 공통 작업에 걸리는 시간이 더 많은 데이터로 고르게 분산되면서 개별 데이터 레코드당 처리 시간이 감소
- 미니배치 주기가 너무 길면 각 미니배치에 필요한 메모리양이 증가

클러스터 리소스를 추가로 투입해 처리 시간을 단축할 수 있음
- 더 많은 메모리를 사용하면 가비지 컬렉션의 빈도를 낮출 수 있음
- 더 많은 CPU 코어를 활용하면 처리 속도를 높일 수 있음

#### 병렬화 확대

모든 CPU 코어를 효율적으로 활용하고 처리량을 늘리려면 결국 병렬화를 확대해야 함. 이는 스트리밍의 여러 단계에서 가능
- 예를 들어 입력 데이터 소스에 적용!
- 카프카의 파티션은 컨슈머가 최대로 확대할 수 있는 병렬화 레벨을 결정
- 다이렉트커넥터는 병렬화 레벨을 자동으로 적용해 스파크 스트리밍의 컨슈머 스레드 개수를 카프카 토픽의 파티션 개수와 동일하게 맞춤
- 리시버 기반 커넥터를 사용한다면 복수의 DStream을 동시에 써서 병렬활 확대가 가능

#### 유입 속도 제한

마지막 처방은 데이터가 유입되는 속도를 제한
- `spark.streaming.receiver.maxRate` 매개변수와 `spark.streaming.kafka.maxRatePerPartition` 매개변수로 데이터의 유입 속도를 수동으로 제한할 수 있음

`spark.streaming.backpressure.enabled` 인자를 true로 설정해 역압력(backpressure) 기능을 활성화할 수 있음

### 6.3.2 장애 내성

#### 실행자의 장애 복구

데이터가 실행자 프로세스에서 구동되는 리시버로 유입되면 스파크는 데이터를 클러스터에 중복 저장함

즉, 리시버의 실행자에 장애가 발생해도 다른 노드에서 실행자를 재시작하고 유실된 데이터를 복구할 수 있음

스파크가 자동으로 해주는 기능

#### 드라이버의 장애 복구


## 6.4 정형 스트리밍

스트리밍 연산의 장애 내성과 일관성을 갖추는 데 필요한 세부 사항을 숨겨서 스티리밍 API를 마치 일괄 처리 API처럼 사용할 수 있게 하는 것

### 6.4.1 스트리밍 DataFrame 생성

### 6.4.2 스트리밍 데이터 출력

### 6.4.3 스트리밍 실행 관리

### 6.4.4 정형 스트리밍의 미래

## 6.5 요약
