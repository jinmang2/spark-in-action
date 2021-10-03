# 3강 스파크 애플리케이션 작성하기
- 스파크 애플리케이션을 작성하는 법을 학습
- Spark 개발자는 대부분 인텔리J나 이클립스(eclipse) 등 통합 개발 환경(IDE)를 사용
- 인텔리J IDEA로 개발하는 방법은 온라인에 많음
- Eclipse는 적음. 그래서 해당 챕터에서는 Eclipse로 스파크 프로그램을 작성하는 법을 학습
    - 자료가 많이 없단 얘기는... 굳이 쓸 이유가 없다는거 아닌가?
    - 처음이니 걍 해보자
- 이클립스 설치 -> 이클립스 플러그인 설치 -> Apache Maven으로 spark application project 생성
    - Apache Maven? SW 프로젝트를 관리하는 도구
    - 깃헙(https://github.com/spark-in-action)에 Maven archetype을 준비해둠
- 이 장에 걸쳐 깃허브의 푸시 이벤트 발생 횟수를 집계하는 예제 애플리케이션을 작성
- 실습엔 스파크 버전 1.3.0에서 공개된 `DataFrame`이라는 새롭고 흥미로운 구조체 사용

## Review

### 1장: 아파치 스파크 소개
- Spark는 메모리 효율적, Map Reduce보다 10~100배 빠름
- Spark collection에 적용한 연산은 복잡한 병렬 프로그램으로 자동 변환되나 사용자가 알 필요 X
- Hadoop은 데이터 분산 처리에서 고민해야 하는 아래 문제를 해결
    - 병렬 처리(Parallelization): 전체 연산을 잘게 나눠 동시 처리
    - 데이터 분산(Distribution): 데이터를 여러 노드로 분산
    - 장애 내성(Fault Tolerance): 분산 컴포넌트의 장애에 대응
- 하지만 Map Reduce는 job의 결과를 다른 job에서 사용하려면 HDFS에 저장해야 함 (한계)
- 스파크의 핵심은 `In-Memory`
- 스파크 API는 맵리듀스 API보다 사용하기 편함
- 통합 플랫폼 제공
- 스파크는 job과 task를 시작하는데 상당한 시간을 소모, 소량의 데이터라면 굳이 사용할 필욘 X
- 스파크 코어 `RDD` 2장에서 다룸
- 스파크가 하둡 생태계를 모두 커버하는 것은 아님
    - 임팔라 드릴, 플럼척와는 일부만 커버
    - 암바리, 우지 HBase 주키퍼는 못함

#### Installation
- https://lovit.github.io/spark/2019/01/29/spark_0_install/

1. 환경
    - 64 bit 우분투 OS, 버전 14.04.4
    - Java 8(OpenJDK)
    - 하둡 2.7.2
    - 스파크 2.0
    - 카프카 0.8.2
2. 가상 머신 시작
    - 오라클 VirtualBox 설치
        - https://code-delivery.me/posts/ubuntu-getting-started/
    - 베이그런트(Vagrant)
        - https://blog.thenets.org/how-to-run-vagrant-on-wsl-2/
        - https://artflower.github.io/blog/2021/05/WSL2-vagrant
        - https://www.44bits.io/ko/post/vagrant-tutorial
        ```
        # run inside WSL 2
        # check https://www.vagrantup.com/downloads for more info
        curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo apt-key add -
        sudo apt-add-repository "deb [arch=amd64] https://apt.releases.hashicorp.com $(lsb_release -cs) main"
        sudo apt-get update && sudo apt-get install vagrant

        # append those two lines into ~/.bashrc
        echo 'export VAGRANT_WSL_ENABLE_WINDOWS_ACCESS="1"' >> ~/.bashrc
        echo 'export PATH="$PATH:/mnt/c/Program Files/Oracle/VirtualBox"' >> ~/.bashrc

        # now reload the ~/.bashrc file
        source ~/.bashrc

        # For WSL2, install WSL2 plugin
        vagrant plugin install virtualbox_WSL2
        # vagrant up
        ```

```
# json 형식의 vagrant box metadata file 내려받기
wget https://raw.githubusercontent.com/spark-in-action/first-edition/master/spark-in-action-box.json
# 가상 머신 내려받기
# 생각보다 오래걸림. 본인은 30분 정도 소요
vagrant box add --name manning/spark-in-action spark-in-action-box.json
```

- 가상환경을 사용하기 전에 먼저 초기화

```
# Vagrantfile 생성
# 명령어 실행 후 아래 두 내용을 추가
# config.vm.network "private_network", ip: "192.168.10.2"
# config.vm.network "public_network"
$ vagrant init manning/spark-in-action

# https://blog.1day1.org/546
# 인터넷에 연결된 인터페이스를 가상 머신과 연결
$ vagrant up
```

- 가상 머신 종료
```
# 가상 머신 종료
$ vagrant halt

# 가상 머신 완전히 제거 및 디스크 여유 공간 확보
$ vagrant destroy

# Vagrantfile 제거
$ vagrant box remove manning/spark-in-action
```

### 2장: 스파크의 기초
- **자체 클러스터**: 메소스나 하둡의 YARN 클러스터 매니저가 아닌 스파크에 내장된 클러스터 매니저
- **로컬**: 스파크의 전체 시스템이 로컬 컴퓨터에서 실행됨

#### Installation

- 가상 머신이 중지됐다면 아래 명령으로 재실행

```
$ vagrant up
```

- 가상 머신에 로그인
    - wundow에선 Putty나 Kitty, MobaXTerm 등을 사용해 가상 머신 IP 주소로 직접 접속 가능

```
$ vagrant ssh -- -l spark
```

- 로그인 프롬프트는 동일.
    - 사용자 이름: spark, 암호: spark
    - 초기암호: https://www.opentutorials.org/module/3613/21660
    - 왜 다르지...? 모르겠다.

```
jinmang2@DESKTOP-029MHGN:/mnt/c/spark$ vagrant ssh -- -l spark
==> default: The machine you're attempting to SSH into is configured to use
==> default: password-based authentication. Vagrant can't script entering the
==> default: password for you. If you're prompted for a password, please enter
==> default: the same password you have configured in the Vagrantfile.
vagrant@172.21.112.1's password:
Welcome to Ubuntu 14.04.4 LTS (GNU/Linux 3.13.0-85-generic x86_64)

 * Documentation:  https://help.ubuntu.com/

 System information disabled due to load higher than 1.0

  Get cloud support with Ubuntu Advantage Cloud Guest:
    http://www.ubuntu.com/business/services/cloud

119 packages can be updated.
71 updates are security updates.

New release '16.04.7 LTS' available.
Run 'do-release-upgrade' to upgrade to it.


Last login: Thu Apr 21 12:10:21 2016 from 10.0.2.2
vagrant@spark-in-action:~$ dir
vagrant@spark-in-action:~$
```

- `vagrant` 치니까 위처럼 잘되더라
- 홈 디렉터리에 깃허브 저장소를 저장

```
vagrant@spark-in-action:~$ git clone https://github.com/spark-in-action/first-edition
```

- java 설치 확인
    - 심볼릭 링크란?: 파일이나 폴더에 대한 일종의 참조(reference)
    - 이를 사용하면 파일 시스템 내 서로 다른 두 위치에서 같은 파일(또는 폴더)에 접근할 수 있음

```
# symbolic link
vagrant@spark-in-action:~$ which java
/usr/bin/java

# 실제로 자바가 설치된 위치
vagrant@spark-in-action:~$ ls -la /usr/bin/java
lrwxrwxrwx 1 root root 22 Apr 19  2016 /usr/bin/java -> /etc/alternatives/java

vagrant@spark-in-action:~$ ls -la /etc/alternatives/java
lrwxrwxrwx 1 root root 46 Apr 19  2016 /etc/alternatives/java -> /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java
```

- 하둡과 스파크 실행에 필요한 JAVA_HOME 환경 변수 미리 설정
```
vagrant@spark-in-action:~$ echo $JAVA_HOME
/usr/lib/jvm/java-8-openjdk-amd64/jre
```

- 가상머신 하둡 사용

```
vagrant@spark-in-action:~$ export PATH=$PATH:/opt/hadoop-2.7.2/bin
vagrant@spark-in-action:~$ hadoop fs -ls /user
21/10/02 13:44:26 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Found 1 items
drwxr-xr-x   - spark supergroup          0 2016-04-19 18:49 /user/spark
```

- 스파크 릴리즈 관리

```
vagrant@spark-in-action:~$ ls /opt | grep spark
spark-1.6.1-bin-hadoop2.6
spark-2.0.0-bin-hadoop2.7
```

- SPARK_HOME 설정값 확인

```
vagrant@spark-in-action:~$ export | grep SPARK
declare -x SPARK_HOME="/usr/local/spark"
```

- 스파크 shell 시작
    - [sudo path 유지](https://brownbears.tistory.com/252)

```
vagrant@spark-in-action:~$ find / -name spark-shell
vagrant@spark-in-action:~$ export PATH=$PATH:./usr/local/spark/bin
vagrant@spark-in-action:~$ spark-shell
```

- Spark Shell에는 오류 로그만 출력, 나머지 로그들은 추후 문제 진단에 사용할 수 있게 log파일에 저장
    - [Tomcat Error](http://www.kunwi.co.kr/os/73?sst=wr_hit&sod=asc&sop=and&page=6)

```
$ sudo nano /usr/local/spark/conf/log4j.properties
```
```
log4j.rootCategory=WARN, console
log4j.logger.org.apache.spark.repl.Main=ERROR

log4j.appender.A2.File=/usr/local/tomcat/logs/catalina.out
```

## 3.1 이클립스로 스파크 프로젝트 생성
인텔리J나 기타 다른 IDE를 사용한다면 건너뛰어도 무방

- 이클립스 설치, https://goo.gl/K3Yq7
- 가상머신 `/home/spark/eclipse` 폴더에 이미 생성
    - `/home/spark/workspace`을 워크스페이스 폴더로 사용
- GUI 사용을 위해 X 윈도 시스템 설정
    - Window에서는 Xming(https://goo.gl.QjwqFt) 사용 가능
- VM 안의 리눅스 셸의 DISPLAY 환경 변수에 X 윈도 시스템 IP 주소  설정

인텔리 J IDEA로 설치
- https://goddaehee.tistory.com/195
- https://opennote46.tistory.com/234
- https://hasoo.github.io/java/wsl2-intellij/

환경 겁나꼬임... 새로 구축
- https://kontext.tech/column/hadoop/307/install-hadoop-320-on-windows-10-using-windows-subsystem-for-linux-wsl
    - https://typingdog.tistory.com/101
- https://kontext.tech/column/spark/311/apache-spark-243-installation-on-windows-10-using-windows-subsystem-for-linux


## 3.2 스파크 애플리케이션 개발
- 목적: 모든 직원 명단과 각 직원이 수행한 푸시 횟수를 담은 일일 리포트를 개발
    - Push? Commit?: 깃 같은 분산 소스 제어 및 관리 시스템에서 변경 사항의 푸시는 로컬 저장소에 저장된 콘텐츠 변경 내역을 원격 저장소로 전송하는 것을 의미
    - 과거 SCM 시스템에서는 이를 커밋으로 칭함

### 3.2.1 깃허브 아카이브 데이터셋 준비

```
$ mkdir -d $HOME/sia/github-archive
$ cd $HOME/sia/github-archive
$ wget http://data.githubarchive.org/2015-03-01-{0..23}.json.gz
```

- 위 명령으로 2015년 3월 1일에 기록된 모든 깃허브 활동 로그를 내려받을 수 있다
- 하루치 로그는 다음과 같이 시간대별로 나누어져 있다

    ```
    2015-03-01-0.json.gz
    2015-03-01-1.json.gz
    ...
    2015-03-01-25.json.gz
    ```

- 아래 명령으로 파일 압축 해제

```
$ gunzip *
```

- 전체 용량은 1GB, 첫 번째 파일만 사용하자 (44MB)
- 해당 파일은 유효한 JSON 파일이 아님
    - line by line으로 유효한 JSON 문자열 여러 개가 한 줄에 하나씩 기록

```
$ head -n 1 2015-03-01-0.json | jq '.'
```
```

{
  "id": "2614896652",
  "type": "CreateEvent",
  "actor": {
    "id": 739622,
    "login": "treydock",
    "gravatar_id": "",
    "url": "https://api.github.com/users/treydock",
    "avatar_url": "https://avatars.githubusercontent.com/u/739622?"
  },
  "repo": {
    "id": 23934080,
    "name": "Early-Modern-OCR/emop-dashboard",
    "url": "https://api.github.com/repos/Early-Modern-OCR/emop-dashboard"
  },
  "payload": {
    "ref": "development",
    "ref_type": "branch",
    "master_branch": "master",
    "description": "",
    "pusher_type": "user"
  },
  "public": true,
  "created_at": "2015-03-01T00:00:00Z",
  "org": {
    "id": 10965476,
    "login": "Early-Modern-OCR",
    "gravatar_id": "",
    "url": "https://api.github.com/orgs/Early-Modern-OCR",
    "avatar_url": "https://avatars.githubusercontent.com/u/10965476?"
  }
}
```

### 3.2.2 JSON 로드
- 스파크 SQL과 `DataFrame`은 JSON 데이터를 스파크로 입수하는 기능 제공
- `DataFrame`의 빠른 성능과 스파크 컴포넌트(스파크 스트리밍, 스파크 MLlib 등) 사이의 원활한 데이터 호환성에 많은 관심을 보임

```
Note: Data-Frame API
- DataFrame은 스키마가 있는 RDD
- 관계형 DB처럼 각 column별로 이름과 타입을 가짐
- Table과 유사
- 스키마 장착 DataFrame의 강점은 계산 최적화
- 스파크는 정형 데이터셋을 DataFrame으로 생성할 때 전체 JSON 데이터셋을 탐색하고 데이터 스키마를 유추
- 그런 다음 데이터 스키마를 참고해 실행 계획을 만들어 더 나은 계산 최적화를 이끌어 낼 수 있음
- 참고로 스파크 버전 1.3.0 이전에는 DataFrame을 SchemaRDD로 칭함
```

- 사용자는 DataFrame으로 SQL과 유사한 문법을 사용하여 보다 서술적인 방식으로 분석 의도 설명 가능
- 하지만 스파크 코어 API로 무언가를 분석하려면 데이터를 어떻게 변형할 지 일일이 지정해야 한다
    - 의미있는 결론에 도달하려면 데이터를 어떻게 재구성해야 할지
- 즉, 스파크 코어는 다른 모든 스파크 컴포넌트를 건설할 수 있는 기초적인 레고 조각의 집합
- DataFrame API로 작성한 코드 또한 일련의 스파크 코어 변환 연산자로 자동 변환
- `DataFrame`은 5장에서 집중적으로 다룸
- 스파크의 메인 인터페이스는 `SQLContext` 클래스 (스파크 코어의 SparkContext와 유사)
- 스파크 v2.0은 `SparkContext`와 `SQLContext`를 `SparkSession` 클래스로 통합
- `SQLContext`의 `read 메서드`는 다양한 데이터를 입수하는 데 사용할 수 있는 `DataFrameReader` 객체를 반환

```
# json method signature
def json(paths: String*): DataFrame
Loads a JSON file (one object per line) and returns the results as a [[DataFrame]].
```
- 위에 시그니쳐를 보면, json 메서드는 한 줄당 JSON 객체 하나가 저장된 파일을 로드
- 이는 깃허브 아카이브 파일 구조와 정확하게 일치

- App.scala 파일 수정

```scala
// App.scala
import org.apache.spark.sql.SparkSession

object App {

  def main(args : Array[String]) {
    // TODO expose appName and master as app. params
    val spark = SparkSession.builder()
        .setAppNameappName("GitHub push counter")
        .setMastermaster("local[*]")
        .getOrCreate()
    val sc = spark.sparkContext
  }
}
```

- App.scala에 아래 코드를 추가하여 첫 번째 JSON 파일을 로드
    - 스칼라 코드 안에서는 파일 결로에 물결표(~)나 $HOME을 사용할 수 없다.
    - 즉, 먼저 HOME 환경 변수를 시스템에서 가져와 JSON 파일의 절대 경로를 구성한다

```scala
val homeDir = System.getenv("HOME")
val inputPath = homeDir + "/sia/github-archive/2015-03-01-0.json"
val ghLog = spark.read.json(inputPath)
```

- json 메서드는 2장에서 학습한 `filter`, `map`, `flatMap`, `collect`,. `count` 등 여러 표준 RDD 메서드를 제공하는 `DataFrame` 객체를 반환

Then,
- 푸시 이벤트만 남도록 로그 항목 필터링
- By scaladocs(http://mng.bz/3EQc), 여러 버전의 filter 함수가 오버로드되어 있음
    - 스파크 v2.0부턴 DataFrame이 Dataset의 일종, 즉 Row 객체를 요소로 포함하는 Dataset으로 변경
- String을 인수로 받는 filter 함수는 주어진 조건식을 SQL 형식으로 파싱

```scala
val pushes = ghLog.filter("type = 'PushEvent'")
```

- 코드가 잘 동작하는지 다음 챕터에서 확인

### 3.2.3 이클립스에서 애플리케이션 실행
- 유감스럽지만 인텔리 J 활용
    - https://blog.voidmainvoid.net/162
- 아래 코드를 추가하여 애플리케이션의 동작을 눈으로 확인

```scala
pushes.printSchema // pushes DataFrame의 스키마를 보기 좋게 출력
println("all events: " + ghLog.count)
println("only pushes: " + pushes.count)
pushes.show(5) // DataFrame의 상위 다섯 개 로우(row)를 테이블 형태로 출력
               // Default 20
```

- 인텔리 J에서 실행하면 에러발생 (dependency)
    - https://weejw.tistory.com/232

### 3.2.4 데이터 집계
- push event를 사용자 이름으로 그루핑하여 각 그룹별 로유 개수를 집계

```scala
val grounded = pushes.groupBy("actor.login").count
grouped.show(5)
```

- count 칼럼을 기준으로 데이터셋을 정렬

```scala
val ordered = grouped.orderBy(grouped("count").desc)
ordered.show(5)
```

### 3.2.5 분석 대상 제외

- 직원 목록을 사용하기 위해 ScalaCollection으로 로드
- 스칼라에선 Set의 random lookup 성능이 Seq보다 빠름
- 다음 명령으로 파일 전체 내용을 Set 객체에 로드 가능

```scala
import scala.io.Source.fromFile

val empPath = homeDir + "/first-edition/ch03/ghEmployees.txt"
// Set() 메서드는 요소가 없는 불변 Set 객체를 생성
// ++ 메서드는 이 Set에 복소 요소를 추가
val employees = Set() ++ (
  // for 표현식은 파일의 각 줄을 읽어 들여서 line 변수에 저장
  for {
    line <- fromFile(empPath).getLines
  } yield line.trim
  // yield는 for loop의 각 사이클별로 값 하나를 임시 컬렉션에 추가
  // 임시 컬렉션은 for 루프가 종료될 때 전체 for 표현식의 결과로 반환된 후 삭제
)
```

- 위는 for comprehension

```
# 직원 Set 생성 과정
for 루프의 각 반복 사이클에서 다음과 같이 진행
    1. 파일에서 다음 줄을 읽는다
    2. 새로운 line 변수를 초기화하고 변수 값에 파일의 현재 줄을 저장
    3. line 변수 값을 가져와 양끝의 공백 문자를 제거(trim)한 후 임시 컬렉션에 추가
마지막 반복 사이클이 끝나면 다음과 같이 진행한다
    1. 앞서 구성한 임시 컬렉션을 for 표현식의 결과로 반환
    2. 비어 있는 Set에 for 표현식의 결과를 추가한다
    3. 이 Set을 employees 변수에 할당한다
```

- filter 메서드로 전체 사용자 이름과 사용자별 푸시 획수를 담은 ordered DataFrame과 비교

```scala
def filter(conditionExpr: String): Dataset
```

- filter 메서드는 주어진 SQL 표현식을 사용해 로우를 필터링

```scala
val oldPeopleDf = peopleDf.filter("age > 15")
```

- 사용자 정의 함수는 SparkSession 클래스의 udf 클래스로 등록

```scala
// 주어진 문자열이 Set 안에 존재하는지?
// isEmp 함수의 인자 및 반환 값 타입을 명시적으로 지정
val isEmp: (String => Boolean) = (arg: String) => employees.contains(arg)
// 스칼라의 타입 추론 기능을 활용하여 간결하게 작성
// contains의 반환값이 함수의 반환값. 즉, bool
val isEmp = user => employees.value.contains(user)
// isEmp 함수를 UDF로 등 (사용자 정의 함수)
val isEmployee = spark.udf.register("isEmpUdf", isEmp)
// 스파크는 UDF에 필요한 객체를 모두 가져와 클러스터에서 실행하는 모든 태스크에 전송
```

### 3.2.6 공유 변수
- 위 예제를 공유 변수없이 실행하면 200회 가까이 반복적으로 수행
- 반면 공유 변수는 클러스터의 각 노드에 **정확히 한 번만** 전송
- 공유 변수는 클러스터 노드 메모리에 자동으로 캐시됨
- BitTorent와 유사한 P2P(peer-to-peer) 프로토콜을 사용해 공유 변수 전파
- 대규모 클러스터는 대용량 변수를 모든 노드에 전송해도 마스터 실행이 크게 지연되지 않음
- 가쉽 프로토콜(gossip protocol)
    - 바이러스나 소문이 퍼지는 것처럼 워커 노드들이 서로 공유 변수를 교환하며 클러스터 전체에 공유 변수를 유기적으로 확산
- `isEmp` 앞에 공유 변수 추가

```scala
val bcEmployees = sc.broadcast(employees) #A Broadcast the employees set
```

- 공유 변수에 접근하려면 반드시 `value` 메서드 사용
- 아래와 같이 변경

```scala
val isEmp = user => bcEmployees.value.contains(user)
```

- 나머진 변경할 필요 X
- 앞서 등록한 isEmployee UDF를 사용해 ordered DataFrame 필터링

```scala
import spark.implicites._
val = filtered = ordered.filter(isEmployee($"login"))
filtered.show()
```

- 실제 애플리케이션을 작성할 때엔 애플리케이션을 검증 및 운영 환경으로 넘기기 전에 `appName`, `appMaster`, `inputPath`, `empPath` 변수를 애플리케이션의 매개변수로 만들어야 함
- 어떤 클러스터에서 애플리케이션을 실행할지는 아무도 모름 (??)
- 매개변수 값은 애플리케이션 외부에서 지정할 것
    - 안그러면 실행 때 마다 컴파일
- 아래와 같이 실행

```
1. 이클립스 툴바에서 Run > Run Configurations를 선택
2. 대화상자 안쪽에서 Scala Application > App$ 선택
3. Run을 클릭해 애플리케이션 실행
```

## 3.3 애플리케이션 제출

- Spark Cluster에서 애플리케이션을 실행
- 실행하려면 모든 의존 라이브러리에 접근 가능해야함
- 애플리케이션을 패키지로 만들고 나면 운영에 반영하기 전에 테스트 가능하도록 검증 팀에 보내는 경우가 많음
- 운영 팀의 검증 환경이 spark-sql 라이브러리를 포함한다고 장담할 수 없음

### 3.3.1 uberjar 빌드
- Spark 프로그램을 운영 환경에서 원활하게 실행할 수 있게 JAR 파일을 추가하는 두 가지 방법
    1. spark-submit 스크립트의 --jars 매개변수에 프로그램에 필요한 JAR 파일을 모두 나열해 실행자로 전송
    2. 모든 의존 라이브러리를 포함하는 uberjar를 빌드
- 책에선 2에 대해 설명

```
# pom.xml에 추가할 내용
<dependency>
  <groupId>org.apache.commons</groupId>
  <artifactId>commons-email</artifactId>
  <version>1.3.1<version>
  <scope>compile</scope>
</dependency>
```

- commons-email 라이브러리엔 mail과 activation 라이브러리가 필요
- mail엔 activation이 필요
- `maven-shade-plugin` 개념!
    - uberjar를 빌드하는데 필요
    - 예제 애플리케이션의 pom.xml에는 이미 설정
- scope: provided, 해당 라이브러리와 이 라이브러리의 모든 의존 라이브러리를 uberjar에서 제외
- scope: compile, 해당 라이브러리가 애플리케이션의 컴파일 및 런타임 단계에서 필요
- pom.xml 작성 후엔 프로젝트 갱신(up)이 필요
- 프로젝트 루트 마우스 우측 버튼 클릭 후
- `Maven > Update Project`

### 3.3.2 애플리케이션 적응력 올리기
- spark-submit 스크립트로 운영환경에서의 실행을 위해 프로그램 코드 일부 수정!
- SparkConf에 애플리케이션 이름과 스파크 마스터를 지정하는 부분 제거
- 빈 SparkConf 객체를 대신 전달해 SparkContext를 생성

```scala
package org.sia.chapter03App

import scala.io.Source.fromFile
import org.apache.spark.sql.SparkSession

object GitHubDay {
  def main(args : Array[String]) {
    // 빈 SparkConf 객체를 대신 전달
    // val spark = SparkSession.builder()
    //    .setAppNameappName("GitHub push counter")
    //    .setMastermaster("local[*]")
    //    .getOrCreate()
    val spark = SparkSession.builder().getOrCreate()

    val sc = spark.sparkContext

    // inputPath의 값을 매개변수로 전달
    // val homeDir = System.getenv("HOME")
    // val inputPath = homeDir + "/sia/github-archive/2015-03-01-0.json"
    val ghLog = spark.read.json(args(0))

    val pushes = ghLog.filter("type = 'PushEvent'")
    val grouped = pushes.groupBy("actor.login").count
    val ordered = grouped.orderBy(grouped("count").desc)

    // Broadcast the employees set
    // empPath 값을 매개변수로 전달
    // val empPath = homeDir + "/first-edition/ch03/ghEmployees.txt"
    val employees = Set() ++ (
      for {
        // 매개변수로 전달하고 있는 것을 확인할 수 있음
        line <- fromFile(args(1)).getLines
      } yield line.trim
    )
    // 공유 변수 지정 부분은 당연히 동일
    val bcEmployees = sc.broadcast(employees)

    import spark.implicits._
    val isEmp = user => bcEmployees.value.contains(user)
    val sqlFunc = spark.udf.register("SetContainsUdf", isEmp)
    val filtered = ordered.filter(sqlFunc($"login"))
    // 찍어주는 부분 삭제
    // filtered.show()
    // 프로그램 결과를 파일로 출력
    filtered.write.format(args(3)).save(args(2))
  }
}
```

- 스파크가 기본으로 지원하는 파일 형식? JSON, Parquet, JDBC
    - Parquet은 데이터와 스키마를 함께 지정하는 columnar 파일 포맷
- 이 외에도 애플리케이션 적응력을 높이려면 아래 인자를 추가로 받아야 한다
    - 입력 JSON 파일 경로
    - 직원 파일 경로
    - 출력 파일 경로
    - 출력 파일 형식
- `uberjar`를 빌드
    - Run As > Run Configuration
    - Maven Build
    - New launch configuration
    - Name 필드에 Build uberjar 입력
    - Variables 클릭
    - 대화상자에서 project_loc 선택
    - OK 클릭
        - 그러면 Base directory 필드에 ${project_loc}가 입력됨
    - Goals 필드에는 clean package를 입력
    - Skip Tests에 체크
    - Apply를 클릭하여 Run Configuration 저장
    - Run을 클릭하면 빌드 실행 가능
- 결과로 target 폴더에 chapter03App-0.0.1-SNAPSHOT.jar 파일 생성

### 3.3.3 spark-submit 사용
- 애플리케이션 제출 방법에 대한 다른 예시
    - http://mng.bz/WT2Y

```
spark-submit \
    --class <main-class> \
    --master <master-url> \
    --deploy-mode <deploy-mode> \
    --conf <key>= <value> \
    ... # other options
    <application-jar> \
    [application-arguments]
```

- spark-submit은 일종의 핼퍼 스크립트
- 스파크 클러스터에서 실행하는 데 사용
- 스파크 루트 디렉터리 아래 bin 폴더 안에 있음

## 정리
- 인텔리J IDEA로 스파크 프로그램을 개발하는 자료는 많음
- 본 책은 이클립스로 실습
- 그러나 본인은 환경 구축 실패... 다음 발표 전까지 다시 도전할 예정
- scala-archetype-sparkinaction 아키타입을 미리 준비
    - 이거 없으니까 뭔가 혼자 구축하기가 어렵더라!
- https//www.githubarchive.org/에서 깃허브 아카이브 데이터를 기간별로 내려받을 수 있음
- 스파크 SQL과 DataFrame을 사용하여 JSON 데이터를 스파크로 입수 가능
- SparkSession의 json 메서드는 JSON 데이터를 입수하는 기능을 제공
- 입력 파일의 각 줄에는 완전한 JSON 객체를 저장
- Dataset의 filter 메서드는 주어진 SQL 표현식을 파싱하고 표현식 조건을 만족하는 데이터의 하위 집합을 반환
- 스파크 애플리케이션을 이클립스에서 바로 실행 가능
- SparkSession 클래스의 udf 메서드로 사용자 정의 함수를 등록할 수 있음
- 공유 변수를 사용하면 변수 하나를 스파크 클러스터의 각 노드에 정확히 한 번만 전송 가능
- maven-shade-plugin을 사용해 애플리케이션의 모든 의존 라이브러리를 포함하는 uberjar를 빌드 가능
- spark-submit 스크립트를 사용해 스파크 애플리케이션을 클러스터에서 실행 가능
